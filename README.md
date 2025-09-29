# 01 Test task for Engineer

# launch

1.  `docker compose up --build`
2.  go to swager `http://0.0.0.0:8000/docs#/default/get_filtered_projects_price_post`
3.  endpoint also post("/price")

```example request
    {
    "line": "Home Delivery zone 3 5 lb"
    }
```

# Info

    •	On each start, all rows are popped from the DB. The DB is erased and populated again with only the new data.

    tried with no result:
    •	Tika
    •	Tesseract
    •	pdfplumber
    •	pdfminer
    •	Converting to images, adding contrast and saturation

Claude PDF Tools — works well, but has a very low free limit.
GPT doesn’t understand that file.
I ended up splitting it into 33 pages to keep only the required info.
Then I shrank it into eleven 3-page files.
After that, I put it into Gemini — it parsed about 2–8 pages, and I parsed the other 4 files with macOS OCR.
Finally, I put it into an app, converted to CSV, and inserted into MongoDB.

At first, I felt like my hands were tied without paid models.

# Extra

shart mongosh inside mongo contaner
`mongosh "mongodb://root:example@127.0.0.1:27017"`

```

```
