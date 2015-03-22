package paxos

import (
	"fmt"
	"log"
	"net/http"
)

type Hello struct {}

func (h Hello) ServeHTTP (
	w http.ResponseWriter,
	r *http.Request) {
	fmt.Fprint(w, "Hello World!")

}

type String string

func (s String) ServeHTTP (
	w http.ResponseWriter,
	r *http.Request) {
	fmt.Fprint(w, s)
}

type Struct struct {
	Greeting string
	Punct string
	Who string
}

func (st *Struct) ServeHTTP (
	w http.ResponseWriter,
	r *http.Request) {
	fmt.Fprint(w, st.Greeting + " " + st.Who + st.Punct)
}

func TestHello() {
        fmt.Printf("Hello World.\n")
		var h Hello
		http.Handle("/hello", h)
		http.Handle("/string", String("I'm a frayed knot."))
		http.Handle("/struct", &Struct{"Hello", ":", "Howard"})
		err := http.ListenAndServe("localhost:4000", nil)
		if err != nil {
			log.Fatal(err)
		}
}
