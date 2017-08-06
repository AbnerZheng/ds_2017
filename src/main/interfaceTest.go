package main

func test(cmd interface{}){
	println(cmd.(int))
}

func test2(i int){
	test(i)
}

func main() {
	test2(1)
}
