.PHONY: scrape

# Very simple command to run the scraping file haha
scrape:
	python scripts/gebiz_scraping.py

## Display file structures and content
tree:
	tree -I '*.png|*.jpg|*.pyc|*.mov|*.mp4|*pycache*|*.doctree|*.rst*|*.js|*.html|*.css'
