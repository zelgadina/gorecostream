// {"url": "http://ura-povara.ru/journal/6-produktov-kotorye-mogut-navredit-zhelchnomu-puzyrju", "state": "checked", "categories": ["good_site"], "category_another": "", "for_main_page": false, "ctime": 1567713280}

// В поле categories указано например, good_site
// Надо его распарсить и обкачать урлы из этого семпла. И сделать для каждой категории текстовый файл, в формате tsv, в котором должен лежать url\ttitle\tdescription

// Пример, файл good_site.tsv
// http://ura-povara.ru/journal/6-produktov-kotorye-mogut-navredit-zhelchnomu-puzyrju  6 продуктов, которые могут навредить желчному пузырю - Ура! Повара  И что есть, чтобы снизить риск воспалений в желчном?
// Парсить надо максимально быстро, с минимумом ресурсов, но так, чтобы не забить канал/не положить сервер. Будет плюсом решение, не используещее внешних библиотек.


// починить юникод

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
    // "unicode/utf8"
)

type Doc struct {
    Url         string   `json:url`
    Categories  []string `json:categories`
    Title       string
    Description string
}

type Writer struct {
    Channel     chan *Doc
    File        *os.File
}

var categories = make(map[string]*Writer)

func createTSV(category string) (file *os.File) {
    file, err := os.Create(fmt.Sprintf("%s.tsv", category))
    checkError("Cannot create file ", err)
    // defer file.Close()
    return
}

func writeToTSV(file *os.File, write chan *Doc) {
    writer := csv.NewWriter(file)
    writer.Comma = '\t'
    defer writer.Flush()
    for {
        doc, ok := <- write
        if !ok {
            close(write)
            file.Close()
            return
        }
        line := []string{doc.Url, doc.Title, doc.Description}
        writer.Write(line)
        if err := writer.Error(); err != nil {
            log.Fatal("Error writing csv: ", err)
        }
    }
}

func selectCategory(snippets chan *Doc) {
    for {
        doc, ok := <-snippets
        if !ok {
            return
        }
        for _, category := range doc.Categories {
            if _, ok := categories[category]; !ok {
                categories[category] = new(Writer)
                categories[category].Channel = make(chan *Doc)
                categories[category].File = createTSV(category)
                go writeToTSV(categories[category].File, categories[category].Channel)
            }
            categories[category].Channel <- doc
        }
    }
}

func checkError(message string, err error) {
    if err != nil {
        log.Fatal(message, err)
    }
}

func getSnippet(urls, snippets chan *Doc) {
    for {
        doc, ok := <- urls
        if !ok {
            close(snippets)
            return
        }
        client := &http.Client {
            Timeout: 30 * time.Second,
        }
        resp, err := client.Get(doc.Url)

        if err != nil {
            log.Println(err)
            continue
        }
        defer resp.Body.Close()

        if resp.StatusCode == 404 {
            log.Println(err)
            continue
        }

        doc.Title, doc.Description = extract(resp.Body)
        
        snippets <- doc
    }
}

func extract(resp io.Reader) (title, description string) {
    z := html.NewTokenizer(resp)
    titleFound := false
    for {
        tt := z.Next()
        switch tt {
        case html.ErrorToken:
            return
        case html.StartTagToken, html.SelfClosingTagToken:
            t := z.Token()
            if t.Data == "title" {
                titleFound = true
            }
            if t.Data == "meta" {
                desc, ok := extractMetaProperty(t, "description")
                if ok {
                    description = desc
                }

                ogTitle, ok := extractMetaProperty(t, "og:title")
                if ok {
                    title = ogTitle
                }

                ogDesc, ok := extractMetaProperty(t, "og:description")
                if ok {
                    description = ogDesc
                }
            }
        case html.TextToken:
            if titleFound {
                t := z.Token()
                title = t.Data
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

func printSnippet(snippets chan *Doc) {
    for snippet := range snippets {
        fmt.Println(snippet.Url, snippet.Title, snippet.Description, snippet.Categories)
    }
}

func readFromFile(urls chan *Doc) {
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


func main() {
    urls := make(chan *Doc)
    snippets := make(chan *Doc)

    go readFromFile(urls)

    for i := 0; i < 5; i++ {
        go getSnippet(urls, snippets)
    }
    selectCategory(snippets)

    // for _, wrttr := range categories {
    //     go writeToTSV(wrttr.File, wrttr.Channel)
    // }
    // printSnippet(snippets)
}
