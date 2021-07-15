// Gorecostream receives a file.jsonl on input, takes an url and a list of categories for each json.
// Then it pumps the title and description for this url
// and writes the url and snippet to the category_name.tsv.

// TODO: add new error handler, fix writing to file (with buffer), check extract title and description, check jsons, more tests.

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
    "io/ioutil"
    "golang.org/x/net/html"
    "golang.org/x/net/html/charset"
    "sync"
    "strings"
)

type Doc struct {
    Url         string   `json:url`
    Categories  []string `json:categories`
    Title       string
    Description string
}

var categories = make(map[string]chan []string)

func ReadFromFile(filename string, urls chan<- *Doc) {
    file, err := os.Open(filename)
    if err != nil {
        log.Fatalln("Cannot open file", err)
    }
    defer file.Close()
    defer close(urls)    

    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
        if err := scanner.Err(); err != nil {
            log.Fatalln("Cannot scan string", err)
        }
        doc := new(Doc)
        json.Unmarshal([]byte(scanner.Text()), &doc)
        if err != nil {
            log.Println("Cannot parse json", err)
            continue
        }
        if len(doc.Categories) == 0 {
            doc.Categories = []string{"without_category"}
        }
        urls <- doc
    }
}

func getSnippet(wg *sync.WaitGroup, urls <-chan *Doc, snippets chan<- *Doc) {
    defer wg.Done()
    headers := make(map[string]string)
    headers["User-Agent"] = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 YaBrowser/20.9.3.189 (beta) Yowser/2.5 Safari/537.36"
    headers["Accept"] = "*/*"
    headers["Accept-Language"] = "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3"
    client := &http.Client {
        Timeout: 30 * time.Second,
    }
    for {
        doc, ok := <- urls
        if !ok {
            return
        }

        req, err := http.NewRequest(http.MethodGet, doc.Url, nil)
        if err != nil {
            log.Println("Failed to create request", err)
            continue
        }

        for k, v := range headers {
            req.Header.Set(k, v)
        }

        resp, err := client.Do(req)
        if err != nil {
            log.Println("Failed to complete request", err)
            continue
        }

        defer resp.Body.Close()

        if resp.StatusCode < 200 && resp.StatusCode >= 300 {
            log.Println(resp.Status)
            continue
        }

        utf8, err := charset.NewReader(resp.Body, resp.Header.Get("Content-Type"))
        if err != nil {
            log.Println("Failed to create new utf8-Reader", err)
            continue
        }
        body, err := ioutil.ReadAll(utf8)
        if err != nil {
            log.Println("Failed to extract body", err)
            continue
        }
        extract(body, doc)
        // err = extract(body, doc)
        // if err != nil {
        //     log.Println("Failed to get title or description", err)
        //     continue
        // }
        
        snippets <- doc
    }
}

// Extracts title and description if possible.
func extract(body []byte, doc *Doc) {
    res := string(body[:])
    z := html.NewTokenizer(strings.NewReader(res))
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

// For each document from the channel, it sends its url and snippet to the channels according to its categories.
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
                categories[category] = make(chan []string, 5)
                wg.Add(1)
                go writeToTSV(wg, category, categories[category])
            }
            categories[category] <- []string{doc.Url, doc.Title, doc.Description}
        }
    }
}

func writeToTSV(wg *sync.WaitGroup, category string, lines <-chan []string) {
    file, err := os.Create(fmt.Sprintf("%s.tsv", category))
     if err != nil {
        log.Fatalln("Cannot create file", err)
    }

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
            log.Fatalln("Error writing tsv:", err)
        }
    }
}

func main() {
    var wg_snippets, wg_files sync.WaitGroup
    urls := make(chan *Doc, 5)
    snippets := make(chan *Doc)

    filename := "5.jsonl"
    go ReadFromFile(filename, urls)

    for i := 0; i < 5; i++ {
        wg_snippets.Add(1)
        go getSnippet(&wg_snippets, urls, snippets)
    }

    go selectCategory(&wg_files, snippets)

    wg_snippets.Wait()
    close(snippets)
    wg_files.Wait()
}
