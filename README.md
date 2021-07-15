# gorecostream

## Лучше кое-как и сегодня, чем идеально и никогда
В TODO в файле (и в тестах) указан фронт работ по допиливанию.

## Тестовое задание

Есть семпл выгрузки из хадупа в 500 строк. Пример:
```
{"url": "http://ura-povara.ru/journal/6-produktov-kotorye-mogut-navredit-zhelchnomu-puzyrju", "state": "checked", "categories": ["good_site"], "category_another": "", "for_main_page": false, "ctime": 1567713280}

```

В поле categories указано, например, good_site.
Семпл надо распарсить и обкачать из него урлы.
Сделать для каждой категории текстовый файл в формате tsv, в котором должны лежать поля вида url\ttitle\tdescription

Например, файл good_site.tsv
```
http://ura-povara.ru/journal/6-produktov-kotorye-mogut-navredit-zhelchnomu-puzyrju  6 продуктов, которые могут навредить желчному пузырю - Ура! Повара  И что есть, чтобы снизить риск воспалений в желчном?
```

Парсить надо максимально быстро, с минимумом ресурсов, но так, чтобы не забить канал/не положить сервер.
Будет плюсом решение, не используещее внешних библиотек.
