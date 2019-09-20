from bs4 import BeautifulSoup
import requests
import re

url = ""
page = requests.get(url, timeout=5)  # connect to website

# Parse into BeautifulSoup
soup = BeautifulSoup(page.content, 'html.parser')
print(soup)

# Find Specific Elements in the Page
regex = re.compile('^tocsection-')
content_lis = soup.find_all('li', attrs={'class': regex})
print(content_lis)

content = []
for li in content_lis:
    content.append(li.getText().split('\n')[0])
print(content)

# Find using find and find_all functions in div class see_also section
see_also_section = soup.find(
    'div', attrs={'class': 'div-col columns column-width'})
see_also_soup = see_also_section.find_all('li')
print(see_also_soup)

see_also = []
for li in see_also_soup:
    # find a tags that have a title and a class
    a_tag = li.find('a', href=True, attrs={'title': True, 'class': False})
    href = a_tag['href']  # get the href attribute
    text = a_tag.getText()  # get the text
    see_also.append([href, text])  # append to array
print(see_also)

# Write Data
with open('content.txt', 'w') as f:
    for i in content:
        f.write(i+"\n")

with open('see_also.csv', 'w') as f:
    for i in see_also:
        f.write(",".join(i)+"\n")