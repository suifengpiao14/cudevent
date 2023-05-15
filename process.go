package autofillcopyfield

import (
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/pkg/errors"
	"github.com/spf13/cast"
)

var pubSub = gochannel.NewGoChannel(
	gochannel.Config{},
	watermill.NewStdLogger(false, false),
)

const (
	TOPIC = "autofillcopyfield"
)

// 默认容器
var defaultContainer = NewContainer(TOPIC, pubSub, pubSub)

func RegisterProcessor(processors ...Processor) {
	defaultContainer.RegisterProcessor(processors...)
}

func GetProcessor(name string) (processor Processor, ok bool) {
	return defaultContainer.GetProcessor(name)
}

func GetContainer() *_Container {
	return defaultContainer
}
func SetContainer(c *_Container) {
	defaultContainer = c
}

type _Container struct {
	processors Processors
	topic      string
	publisher  message.Publisher
	subscriber message.Subscriber
}

func NewContainer(topic string, publisher message.Publisher, subscriber message.Subscriber) (container *_Container) {

	container = &_Container{
		topic:      topic,
		publisher:  publisher,
		subscriber: subscriber,
	}
	return
}

type Processors []Processor

func (pros *Processors) Add(processors ...Processor) {
	for _, nh := range processors {
		exists := false
		for _, h := range *pros {
			if h.GetName() == nh.GetName() {
				exists = true
				break
			}
		}
		if !exists {
			*pros = append(*pros, nh)
		}
	}
}

func (pros Processors) GetProcessor(name string) (processor Processor, ok bool) {
	for _, h := range pros {
		if h.GetName() == name {
			return h, true
		}
	}
	return nil, false
}

type Processor interface {
	Exec(processMessage ProcessMessage, dst interface{}) (err error)
	GetName() string
}

const (
	PROCESS_SQL       = "sql"
	PROCESS_SQL_EXEC  = "sql_exec"
	PROCESS_HTTP      = "http"
	PROCESS_HTTP_EXEC = "http_exec"
)

type EventType string

const (
	EVENT_TYPE_CREATING EventType = "creating"
	EVENT_TYPE_CREATED  EventType = "created"
	EVENT_TYPE_UPDATING EventType = "updating"
	EVENT_TYPE_UPDATED  EventType = "updated"
	EVENT_TYPE_DELETING EventType = "deleting"
	EVENT_TYPE_DELETED  EventType = "deleted"
)

type Event struct {
	ModelName string    `json:"name"`
	Type      EventType `json:"type"`
	SourceID  Fields    `json:"primary"`
	OldAttr   Fields    `json:"old"`
	NewAttr   Fields    `json:"new"`
}

func (e Event) GetIdentify() (identify string) {
	identify = fmt.Sprintf("%s_%s", e.ModelName, e.Type)
	return identify
}

type EventWithSub struct {
	Type   EventType      `json:"type"`
	Source ProcessMessage `json:"source"`
}

type RunContext struct {
	Name         string       `json:"name"`
	Config       string       `json:"config"`
	Script       string       `json:"script"`
	OutputSchema string       `json:"outputSchema"`
	Input        Fields       `json:"input"`
	Dependencies []RunContext `json:"Dependencies"`
}

type ProcessMessage struct {
	ProcessName string       `json:"processName"`
	Input       Fields       `json:"input"`
	RunContexts []RunContext `json:"runContexts"` // 执行脚本上下文
}

type ProcessMessages []ProcessMessage

type Field struct {
	Name  string `json:"name"`
	Value string `json:"value"`
	Type  string `json:"type"`
}

type Fields []Field

func (fields Fields) Map() (m map[string]interface{}) {
	m = make(map[string]interface{})
	for _, field := range fields {
		var value interface{}
		switch field.Type {
		case "int":
			value = cast.ToInt(field.Value)
		case "float":
			value = cast.ToFloat64(field.Value)
		case "bool":
			value = cast.ToBool(field.Value)
		default:
			value = field.Value
		}
		m[field.Name] = value
	}
	return m
}

// 注册处理器
func (c *_Container) RegisterProcessor(processors ...Processor) {
	if c.processors == nil {
		c.processors = make(Processors, 0)
	}
	c.processors.Add(processors...)
}
func (c *_Container) GetProcessor(name string) (processor Processor, ok bool) {
	return c.processors.GetProcessor(name)
}

// 处理事件
func (c _Container) Work(event EventWithSub) (err error) {
	processor, ok := c.processors.GetProcessor(event.Source.ProcessName)
	if ok {
		err = errors.WithMessagef(err, "processor name:%s", event.Source.ProcessName)
		return err
	}
	var dst interface{}
	err = processor.Exec(event.Source, dst)
	if err != nil {
		return err
	}
	return nil
}
