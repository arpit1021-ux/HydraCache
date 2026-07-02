package protocol

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

type Response struct {
	Type    ResponseType
	Str     string
	Integer int64
	Data    []byte
	Items   []*Response
}

type ResponseType int

const (
	ResponseSimpleString ResponseType = iota
	ResponseError
	ResponseInteger
	ResponseBulkString
	ResponseArray
	ResponseNull
)

func (r *Response) String() string {
	switch r.Type {
	case ResponseSimpleString:
		return r.Str
	case ResponseError:
		return "(error) " + r.Str
	case ResponseInteger:
		return fmt.Sprintf("(integer) %d", r.Integer)
	case ResponseBulkString:
		if r.Data == nil {
			return "(nil)"
		}
		return string(r.Data)
	case ResponseArray:
		if r.Items == nil {
			return "(nil)"
		}
		result := ""
		for i, item := range r.Items {
			result += fmt.Sprintf("%d) %s\n", i+1, item.String())
		}
		return result
	case ResponseNull:
		return "(nil)"
	default:
		return ""
	}
}

type Decoder struct {
	reader *bufio.Reader
}

func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{reader: bufio.NewReader(r)}
}

func (d *Decoder) Decode() (*Response, error) {
	line, err := d.readLine()
	if err != nil {
		return nil, err
	}

	if len(line) == 0 {
		return nil, fmt.Errorf("empty response")
	}

	switch line[0] {
	case '+':
		return &Response{
			Type: ResponseSimpleString,
			Str:  line[1:],
		}, nil

	case '-':
		return &Response{
			Type: ResponseError,
			Str:  line[1:],
		}, nil

	case ':':
		n, err := strconv.ParseInt(line[1:], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid integer: %w", err)
		}
		return &Response{
			Type:    ResponseInteger,
			Integer: n,
		}, nil

	case '$':
		length, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, fmt.Errorf("invalid bulk string length: %w", err)
		}
		if length < 0 {
			return &Response{Type: ResponseNull}, nil
		}
		data := make([]byte, length+2)
		_, err = io.ReadFull(d.reader, data)
		if err != nil {
			return nil, fmt.Errorf("failed to read bulk string: %w", err)
		}
		return &Response{
			Type: ResponseBulkString,
			Data: data[:length],
		}, nil

	case '*':
		count, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, fmt.Errorf("invalid array count: %w", err)
		}
		if count < 0 {
			return &Response{Type: ResponseNull}, nil
		}
		items := make([]*Response, count)
		for i := 0; i < count; i++ {
			item, err := d.Decode()
			if err != nil {
				return nil, fmt.Errorf("failed to decode array item %d: %w", i, err)
			}
			items[i] = item
		}
		return &Response{
			Type:  ResponseArray,
			Items: items,
		}, nil

	default:
		return nil, fmt.Errorf("unknown response type: %c", line[0])
	}
}

func (d *Decoder) readLine() (string, error) {
	var line []byte
	for {
		part, isPrefix, err := d.reader.ReadLine()
		if err != nil {
			return "", err
		}
		line = append(line, part...)
		if !isPrefix {
			break
		}
	}
	return string(line), nil
}
