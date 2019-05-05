package memstore

import "errors"

type Memstore map[string]string

func New() Memstore {
	return make(map[string]string)
}

func (m Memstore) Get(key string) (string, error) {
	value, ok := m[key]
	if !ok {
		return "", errors.New("not found")
	} else {
		return value, nil
	}
}

func (m Memstore) Set(key, value string) error {
	m[key] = value
	return nil
}

func (m Memstore) Delete(key string) error {
	delete(m, key)
	return nil
}
