package protocol

import (
	"fmt"
	"io"
	"strconv"
)

type Encoder struct {
	writer io.Writer
}

func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{writer: w}
}

func (e *Encoder) WriteSimpleString(s string) error {
	_, err := fmt.Fprintf(e.writer, "+%s\r\n", s)
	return err
}

func (e *Encoder) WriteError(msg string) error {
	_, err := fmt.Fprintf(e.writer, "-%s\r\n", msg)
	return err
}

func (e *Encoder) WriteInteger(n int64) error {
	_, err := fmt.Fprintf(e.writer, ":%d\r\n", n)
	return err
}

func (e *Encoder) WriteBulkString(data []byte) error {
	if data == nil {
		_, err := fmt.Fprintf(e.writer, "$-1\r\n")
		return err
	}
	_, err := fmt.Fprintf(e.writer, "$%d\r\n%s\r\n", len(data), data)
	return err
}

func (e *Encoder) WriteBulkStringRaw(s string) error {
	return e.WriteBulkString([]byte(s))
}

func (e *Encoder) WriteArray(items []string) error {
	if items == nil {
		_, err := fmt.Fprintf(e.writer, "*-1\r\n")
		return err
	}
	_, err := fmt.Fprintf(e.writer, "*%d\r\n", len(items))
	if err != nil {
		return err
	}
	for _, item := range items {
		if err := e.WriteBulkString([]byte(item)); err != nil {
			return err
		}
	}
	return nil
}

func (e *Encoder) WriteArrayLen(n int) error {
	_, err := fmt.Fprintf(e.writer, "*%d\r\n", n)
	return err
}

func (e *Encoder) WriteNull() error {
	_, err := fmt.Fprintf(e.writer, "$-1\r\n")
	return err
}

func (e *Encoder) WriteOK() error {
	return e.WriteSimpleString("OK")
}

func (e *Encoder) WriteInt(value int) error {
	return e.WriteInteger(int64(value))
}

func (e *Encoder) WriteFloat(f float64) error {
	s := strconv.FormatFloat(f, 'f', -1, 64)
	return e.WriteBulkStringRaw(s)
}

func (e *Encoder) WriteRaw(data []byte) error {
	_, err := e.writer.Write(data)
	return err
}

func (e *Encoder) Flush() error {
	if f, ok := e.writer.(interface{ Flush() error }); ok {
		return f.Flush()
	}
	return nil
}
