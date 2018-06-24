package text

import (
	"errors"
	"reflect"
	"strings"
	"time"

	"github.com/eaciit/toolkit"
)

type TextObjSetting struct {
	Delimeter   rune
	UseSign     bool
	Signs       [][]rune
	DateFormats map[string]string
}

func NewTextObjSetting(delimeter rune) *TextObjSetting {
	cfg := new(TextObjSetting)
	cfg.Delimeter = delimeter
	cfg.UseSign = true
	cfg.Signs = [][]rune{[]rune{'"'}, []rune{'\''}}
	return cfg
}

func (t *TextObjSetting) SetUseSign(b bool) *TextObjSetting {
	t.UseSign = b
	return t
}

func (t *TextObjSetting) SetSign(runes ...rune) *TextObjSetting {
	if len(runes) == 1 {
		t.Signs = append(t.Signs, []rune{runes[0]})
	} else if len(runes) > 2 {
		t.Signs = append(t.Signs, []rune{runes[0], runes[1]})
	}
	return t
}

func (t *TextObjSetting) SetDateFormat(key, value string) *TextObjSetting {
	if t.DateFormats == nil {
		t.DateFormats = map[string]string{}
	}
	t.DateFormats[key] = value
	return t
}

func (t *TextObjSetting) DateFormat(key string) string {
	if t.DateFormats == nil {
		t.DateFormats = map[string]string{}
	}
	f, ok := t.DateFormats[key]
	if ok {
		return f
	} else {
		f, ok = t.DateFormats[""]
		return f
	}
}

func textToObj(txt string, out interface{}, cfg *TextObjSetting, headers ...string) error {
	vt := reflect.Indirect(reflect.ValueOf(out)).Type()
	//fmt.Println("Kind:", vt.Kind())
	if len(headers) == 0 && vt.Kind() == reflect.Struct {
		for i := 0; i < vt.NumField(); i++ {
			name := vt.FieldByIndex([]int{i}).Name
			headers = append(headers, name)
		}
	}

	var closeQuote rune
	var processBufferToObj bool

	inQuote := false
	txtBuff := ""
	idx := 0

	for _, char := range txt {
		addRune := true
		processBufferToObj = false
		if cfg.UseSign {
			if !inQuote {
				for _, sign := range cfg.Signs {
					if len(sign) > 0 {
						if char == sign[0] {
							inQuote = true
							if len(sign) == 1 {
								closeQuote = char
							} else {
								closeQuote = sign[1]
							}

							addRune = false
							break
						}
					}
				}
			} else if char == closeQuote {
				inQuote = false
				addRune = false
			}
		}

		if char == cfg.Delimeter && !inQuote {
			addRune = false
			processBufferToObj = true
		}

		if addRune {
			txtBuff += string(char)
		}

		if processBufferToObj {
			fieldname := ""
			if idx < len(headers) {
				fieldname = headers[idx]
			} else {
				fieldname = toolkit.ToString(idx)
			}
			processTxtToObjField(txtBuff, out, fieldname, cfg)
			txtBuff = ""
			idx++
		}
	}

	//-- process buff if last operation doesnt does it
	if !processBufferToObj {
		fieldname := ""
		if idx < len(headers) {
			fieldname = headers[idx]
		} else {
			fieldname = toolkit.ToString(idx)
		}
		processTxtToObjField(txtBuff, out, fieldname, cfg)
	}

	return nil
}

func processTxtToObjField(txt string, obj interface{}, fieldname string, cfg *TextObjSetting) error {
	rv := reflect.Indirect(reflect.ValueOf(obj))
	rt := rv.Type()

	var objField interface{}

	if rt.Kind() == reflect.Map {
		keyType := rt.Key()
		if keyType.Kind() == reflect.String {
			//--- first convert to time.Time
			dateFormat := cfg.DateFormat(fieldname)
			objField = toolkit.String2Date(txt, dateFormat)
			txtStr := toolkit.Date2String(objField.(time.Time), dateFormat)

			//--- if fails then do number float
			if txtStr != txt {
				number := toolkit.ToFloat64(txt, 4, toolkit.RoundingAuto)
				if number != float64(0) {
					objField = number
				} else if number == 0 && txt == "0" {
					objField = float64(0)
				} else {
					//--- if fails then do string
					objField = txt
				}
			}

			rv.SetMapIndex(reflect.ValueOf(fieldname), reflect.ValueOf(objField))
		} else {
			return errors.New("output type is a map and need to have string as its key")
		}
	} else {
		numfield := rt.NumField()
		for fieldIdx := 0; fieldIdx < numfield; fieldIdx++ {
			rft := rt.FieldByIndex([]int{fieldIdx})
			if strings.ToLower(rft.Name) == strings.ToLower(fieldname) {
				castResult := func() string {
					defer func() {
						if r := recover(); r != nil {
							//-- do nothing
						}
					}()

					typeName := rft.Type.String()
					objField = textToInterface(txt, typeName, cfg.DateFormat(fieldname))

					rfv := rv.FieldByIndex([]int{fieldIdx})
					rfv.Set(reflect.ValueOf(objField))

					return "OK"
				}()
				if castResult != "OK" {
					return toolkit.Errorf("unable to cast %s to %s", txt, rft.Type.String())
				}
				break
			}
		}
	}

	return nil
}

func textToInterface(txt string, typeName string, dateFormat string) interface{} {
	var objField interface{}
	if typeName == "string" {
		objField = txt
	} else if strings.HasPrefix(typeName, "int") && typeName != "interface{}" {
		objField = toolkit.ToInt(txt, toolkit.RoundingAuto)
	} else if typeName == "float32" {
		objField = toolkit.ToFloat32(txt, 4, toolkit.RoundingAuto)
	} else if typeName == "float64" {
		objField = toolkit.ToFloat64(txt, 4, toolkit.RoundingAuto)
	} else if typeName == "time.Time" {
		objField = toolkit.ToDate(txt, dateFormat)
	} else {
		objField = ""
	}
	return objField
}

func objToText(data interface{}, cfg *TextObjSetting) (string, error) {
	return "", nil
}
