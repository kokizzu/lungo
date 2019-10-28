package bsonkit

// Set is set of unique documents.
type Set struct {
	// List is the list of documents.
	List List `bson:"list"`

	// Index maps documents to their index in the list.
	Index map[Doc]int `bson:"index"`
}

// NewSet returns a new set from the specified list.
func NewSet(list List) *Set {
	// create set
	set := &Set{
		Index: map[Doc]int{},
	}

	// add documents
	for _, doc := range list {
		set.Add(doc)
	}

	return set
}

// Add will add the document to set if has not already been added. It may return
// false if the document has already been added.
func (s *Set) Add(doc Doc) bool {
	// check if already added
	if _, ok := s.Index[doc]; ok {
		return false
	}

	// append document
	s.List = append(s.List, doc)
	s.Index[doc] = len(s.List) - 1

	return true
}

// Replace will replace the first document with the second. It may return false
// if the first document has not been added and the second already has been added.
func (s *Set) Replace(d1, d2 Doc) bool {
	// get index
	index, ok := s.Index[d1]
	if !ok {
		return false
	}

	// check existence
	if _, ok := s.Index[d2]; ok {
		return false
	}

	// replace document
	s.List[index] = d2

	// update index
	delete(s.Index, d1)
	s.Index[d2] = index

	return true
}

// Remove will remove the document from the set. It may return false if the
// document has not been added to the set.
func (s *Set) Remove(doc Doc) bool {
	// check if document has been added
	i, ok := s.Index[doc]
	if !ok {
		return false
	}

	// remove document
	s.List = append(s.List[:i], s.List[i+1:]...)
	delete(s.Index, doc)

	// update index
	for ; i < len(s.List); i++ {
		s.Index[s.List[i]] = i
	}

	return true
}

// Clone will clone the set. Mutating the new set will not mutate the original
// set.
func (s *Set) Clone() *Set {
	// prepare clone
	clone := &Set{
		List:  make(List, len(s.List)),
		Index: map[Doc]int{},
	}

	// copy list
	copy(clone.List, s.List)

	// copy index
	for doc, index := range s.Index {
		clone.Index[doc] = index
	}

	return clone
}
