package docs

import (
	"bufio"
	"fmt"
	"go/ast"
	"go/doc"
	"go/parser"
	"go/token"
	"os"
	"text/template"
)

// HomeExamples houses examples for home.md
type HomeExamples struct {
	HCLFullExample  string
	BashFullExample string
}

func templateHome() {

	homeTmpl, err := os.ReadFile("templates/home.md.tmpl")
	if err != nil {
		panic(err)
	}

	hclFull, err := os.ReadFile("assets/hcl_full_example.hcl")
	if err != nil {
		panic(err)
	}

	envFull, err := os.ReadFile("assets/env_full_example.sh")
	if err != nil {
		panic(err)
	}

	HomeEg := HomeExamples{string(hclFull), string(envFull)}

	t := template.Must(template.New("home").Parse(string(homeTmpl)))

	f, err := os.Create("out/home.md")
	if err != nil {
		panic(err)
	}

	defer f.Close()

	w := bufio.NewWriter(f)

	writeErr := t.Execute(w, HomeEg)
	if writeErr != nil {
		panic(writeErr)
	}

	flushErr := w.Flush()
	if flushErr != nil {
		panic(flushErr)
	}
}

// TargetExamples TODO: shut linter up here
type TargetExamples struct {
	HTTPTargetConfig string
}

// TemplateTarget TODO
func TemplateTarget() {
	// New jank approach: run go doc target HTTPTargetConfig > assets/httpTargetdoc.txt first.

	targetTmpl, err := os.ReadFile("templates/targets.md.tmpl")
	if err != nil {
		panic(err)
	}

	HTTPTargetConfigDef, err := os.ReadFile("assets/httpTargetdoc.txt")
	if err != nil {
		panic(err)
	}

	if len(HTTPTargetConfigDef) < 1 {
		panic("no definition found for HTTPTargetConfigDef") // TODO: unjank this.
	}

	// fmt.Println(string(HTTPTargetConfigDef))
	TargetEg := TargetExamples{string(HTTPTargetConfigDef)}

	t := template.Must(template.New("target").Parse(string(targetTmpl)))

	f, err := os.Create("out/targets.md")
	if err != nil {
		panic(err)
	}

	defer f.Close()

	w := bufio.NewWriter(f)

	writeErr := t.Execute(w, TargetEg)
	if writeErr != nil {
		panic(writeErr)
	}

	flushErr := w.Flush()
	if flushErr != nil {
		panic(flushErr)
	}
}

////

///
func templateTargetOld() {

	/*
			// src and test are two source files that make up
			// a package whose documentation will be computed.
			const src = `
		// This is the package comment.
		package p

		import "fmt"

		// This comment is associated with the Greet function.
		func Greet(who string) {
			fmt.Printf("Hello, %s!\n", who)
		}
		`
			const test = `
		package p_test

		// This comment is associated with the ExampleGreet_world example.
		func ExampleGreet_world() {
			Greet("world")
		}
		`
	*/

	// Create the AST by parsing src and test.
	fset := token.NewFileSet()
	files := []*ast.File{
		mustParse(fset, "../pkg/target/http.go"),
	}

	// Compute package documentation with examples.
	p, err := doc.NewFromFiles(fset, files, "example.com/p")
	if err != nil {
		panic(err)
	}

	fmt.Printf("package %s - %s", p.Name, p.Doc)

	fmt.Println(p.Filenames)
	fmt.Println(p.Funcs)

	for _, tp := range p.Types {

		/*
			err := format.Node(os.Stdout, fset, tp)
			if err != nil {
				panic(err)
			}
		*/

		fmt.Println(tp.Name)
		fmt.Println(tp.Doc)
		fmt.Println(tp.Consts)
		fmt.Println(tp.Vars)
		fmt.Println(tp.Funcs)
		fmt.Println(tp.Methods)
		fmt.Println(tp.Decl.Doc)
		fmt.Println(tp.Decl.TokPos)
		fmt.Println(tp.Decl.Tok)
		fmt.Println(tp.Decl.Lparen)
		fmt.Println(tp.Decl.Rparen)

		fmt.Println(tp.Decl.Specs[0])
		fmt.Println(tp.Decl.Specs[0])
	}
	// fmt.Printf("func %s - %s", p.Funcs[0].Name, p.Funcs[0].Doc)
	// fmt.Printf(" ⤷ example with suffix %q - %s", p.Funcs[0].Examples[0].Suffix, p.Funcs[0].Examples[0].Doc)

	// This approach needs time to figure out. Let's do something a bit more jank for demo purposes...
}

func mustParse(fset *token.FileSet, filename string) *ast.File {
	f, err := parser.ParseFile(fset, filename, nil, parser.ParseComments)
	if err != nil {
		panic(err)
	}
	return f
}
