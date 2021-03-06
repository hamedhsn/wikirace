from bs4 import BeautifulSoup
from wikirace.configuration import DMN

try:
    # For Python 3.0 and later
    from urllib.request import urlopen
except ImportError:
    # Fall back to Python 2's urllib2
    from urllib2 import urlopen


def make_wiki_link(url):
    """ create the full link by adding wiki domain

    :param url:
    :return:
    """
    if url.startswith('/wiki'):
        return '{}{}'.format(DMN, url)
    else:
        return url


def get_links(url):
    """ retrieve all wiki links inside a url

    :param url: url to fetch the links
    :return:
    """
    print(url)
    try:
        page = urlopen(url)
    except:
        return []

    soup = BeautifulSoup(page.read())
    links_tmp = soup.findAll("a")
    links = list()

    for link in links_tmp:
        if str(link.attrs.get('href')).startswith('/wiki') and not link.attrs.get('class'):
            links.append({
                'url': link.attrs.get('href'),
                'title': link.attrs.get('title')
            })

    return links

if __name__ == '__main__':
    url = 'https://en.wikipedia.org/wiki/Western_Collegiate_Hockey_Association'
    urls = get_links(url)
    print(urls)
    print(len(urls))
