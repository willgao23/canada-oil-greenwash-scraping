import fetcher
import reader

if __name__ == "__main__":
    print("Scraping pipeline started")
    # fetcher.fetch_urls()
    # fetcher.fetch_pdfs()
    print("Links and PDFs Fetched!")
    # reader.read_urls()
    reader.retry_failed_pdfs()
    print("Raw Text Scraped!")
