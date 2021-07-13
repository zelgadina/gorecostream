package main

import (
    "fmt"
    "os"
    "log"
    "bufio"
    "encoding/json"
    "net/http"
    "encoding/csv"
    "time"
    "io"
    "golang.org/x/net/html"
    "sync"
    // "unicode/utf8"
)

type Doc struct {
    Url         string   `json:url`
    Categories  []string `json:categories`
    Title       string
    Description string
}

var categories = make(map[string]chan []string)

func readFromFile(urls chan<- *Doc) {
    file, err := os.Open("50.jsonl")
    if err != nil {
        log.Fatal(err)
    }
    defer file.Close()

    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
        doc := new(Doc)
        json.Unmarshal([]byte(scanner.Text()), &doc)

        if len(doc.Categories) == 0 {
            doc.Categories = []string{"without_category"}
        }
        urls <- doc
    }
    if err := scanner.Err(); err != nil {
        log.Fatal(err)
    }
    close(urls)
}

func getSnippet(wg *sync.WaitGroup, urls <-chan *Doc, snippets chan<- *Doc) { 
    defer wg.Done()
    for {
        doc, ok := <- urls
        if !ok {
            return
        }
        client := &http.Client {
            Timeout: 30 * time.Second,
        }
        resp, err := client.Get(doc.Url)

        // if err != nil {
        //     log.Println(err)
        //     continue
        // }
        defer resp.Body.Close()

        switch resp.StatusCode {
        case 404:
            log.Println(err)
            continue
        case 503:
            log.Println(err)
            continue
        }
        extract(resp.Body, doc)
        
        snippets <- doc
    }
}

func extract(resp io.Reader, doc *Doc) {
    z := html.NewTokenizer(resp)
    titleFound := false
    for {
        tt := z.Next()
        switch tt {
        case html.ErrorToken:
            return
        case html.StartTagToken, html.SelfClosingTagToken:
            t := z.Token()
            if t.Data == `body` {
                return
            }
            if t.Data == "title" {
                titleFound = true
            }
            if t.Data == "meta" {
                desc, ok := extractMetaProperty(t, "description")
                if ok {
                    doc.Description = desc
                }

                ogTitle, ok := extractMetaProperty(t, "og:title")
                if ok {
                    doc.Title = ogTitle
                }

                ogDesc, ok := extractMetaProperty(t, "og:description")
                if ok {
                    doc.Description = ogDesc
                }
            }
        case html.TextToken:
            if titleFound {
                t := z.Token()
                doc.Title = t.Data
                titleFound = false
            }
        }
    }
    return
}

func extractMetaProperty(t html.Token, prop string) (content string, ok bool) {
    for _, attr := range t.Attr {
        if attr.Key == "property" && attr.Val == prop {
            ok = true
        }

        if attr.Key == "content" {
            content = attr.Val
        }
    }
    return
}

func selectCategory(wg *sync.WaitGroup, snippets <-chan *Doc) {
    for {
        doc, ok := <-snippets
        if !ok {
            for _, category := range categories {
                close(category)
            }
            return
        }
        for _, category := range doc.Categories {
            if _, ok := categories[category]; !ok {
                categories[category] = make(chan []string)
                wg.Add(1)
                go writeToTSV(wg, category, categories[category])
            }
            categories[category] <- []string{doc.Url, doc.Title, doc.Description}
        }
    }
}

func writeToTSV(wg *sync.WaitGroup, category string, lines <-chan []string) {
    file, err := os.Create(fmt.Sprintf("%s.tsv", category))
    checkError("Cannot create file ", err)

    writer := csv.NewWriter(file)
    writer.Comma = '\t'
    // defer writer.Flush()
    defer wg.Done()
    defer file.Close()

    for {
        line, ok := <- lines
        if !ok {
            return
        }
        writer.Write(line)
        writer.Flush()
        if err := writer.Error(); err != nil {
            log.Fatal("Error writing tsv: ", err)
        }
    }
}


func checkError(message string, err error) {
    if err != nil {
        log.Fatal(message, err)
    }
}

func printSnippet(snippets <-chan *Doc) {
    for snippet := range snippets {
        fmt.Println(snippet.Url, snippet.Title, snippet.Description, snippet.Categories)
    }
}

func main() {
    var wg_snippets, wg_files sync.WaitGroup
    urls := make(chan *Doc, 10)
    snippets := make(chan *Doc)

    go readFromFile(urls)

    for i := 0; i < 3; i++ {
        wg_snippets.Add(1)
        go getSnippet(&wg_snippets, urls, snippets)
    }

    go selectCategory(&wg_files, snippets)

    wg_snippets.Wait()
    close(snippets)
    wg_files.Wait()
}
