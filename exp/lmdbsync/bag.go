package lmdbsync

type Bag interface {
	Value(key interface{}) interface{}
}

type bag struct {
	b Bag
	m map[interface{}]interface{}
}

func (b *bag) Value(key interface{}) interface{} {
	v, ok := b.m[key]
	if ok {
		return v
	}
	if b.b == nil {
		return nil
	}
	return b.b.Value(key)
}

func Background() Bag {
	return &bag{}
}

func BagWith(b Bag, key, value interface{}) Bag {
	var m map[interface{}]interface{}
	if _b, ok := b.(*bag); !ok {
		if value != nil {
			m = map[interface{}]interface{}{key: value}
		}
	} else {
		// collapse the bag value to reduce indirection
		b = _b.b
		m = make(map[interface{}]interface{}, len(_b.m)+1)
		for k, v := range _b.m {
			m[k] = v
		}
		if value != nil {
			m[key] = value
		}
	}
	return &bag{
		b: b,
		m: m,
	}
}
