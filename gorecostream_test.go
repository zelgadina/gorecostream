// TODO: add tests for checking bad file, empty file, bad json, bad network connection, http-errors, empty snippets, etc.

package main_test

import (
    "testing"
    "github.com/zelgadina/gorecostream"
)

var testUrls = []string{
    "http://www.spb.aif.ru/society/people/marshal_govorov_bez_prava_na_oshibku",
    "http://femmie.ru/kak-znaki-zodiaka-spravlyayutsya-s-odinochestvom-225020",
    "http://rns.online/society/Vitse-premera-po-evrointegratsii-Ukraini-ne-pustili-na-sammit-s-ES-2019-07-08",
    "https://tvzvezda.ru/news/forces/content/201907311315-mil-ru-s54zy.html",
    "http://sports.ru/tribuna/blogs/mama4h/2484015.html",
}

var testCategories = [][]string{
    []string{"without_category"},
    []string{"good_site", "hard"},
    []string{"good_site", "lolkek"},
    []string{"without_category"},
    []string{"good_site"},
}

func Test01ReadUrl(t *testing.T) {
    urls := make(chan *main.Doc, 2)
    go main.ReadFromFile("5.jsonl", urls)
    for i := 0; i < 6; i++ {
        doc, ok := <- urls
        switch {
        case i >= 5 && ok:
            t.Errorf("%d element with %s value is out of range; expected: end of file", i, doc.Url)
        case !ok:
            break
        case doc.Url != testUrls[i]:
            t.Errorf("Read URL is %s; want %s", doc.Url, testUrls[i])

        }
    }
}

func Test02ReadCategories(t *testing.T) {
    categories := make(chan *main.Doc, 2)
    go main.ReadFromFile("5.jsonl", categories)
    for i := 0; i < 6; i++ {
        doc, ok := <- categories
        switch {
        case i >= 5 && ok:
            t.Errorf("%d element with %s value is out of range; expected: end of file", i, doc.Categories)
        case !ok:
            break
        default:
            for j, categ := range doc.Categories {
                if categ != testCategories[i][j] {
                    t.Errorf("Read categories is %s; want %s", doc.Categories, testCategories[i])
                }
            }

        }
    }
}
