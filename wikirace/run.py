from wikirace.configuration import INGESTION_TOPIC, OUTPUT_COLNM, MAX_DEPTH
from wikirace.utils.bfilter import BloomFilter
from wikirace.utils.kfkpywrapper import KfkConsumer, KfkProducer
from wikirace.utils.mongo import mongo_connect
from wikirace.utils.string import hash_str
import copy
import concurrent.futures as ft

# cons = KfkConsumer(INGESTION_TOPIC, 'con', set_offset_to_end=True)
from wikirace.utils.urls import make_wiki_link, get_links

cons = KfkConsumer(INGESTION_TOPIC, 'con')
prod = KfkProducer(INGESTION_TOPIC)
dbcon_oput = mongo_connect(col_nm=OUTPUT_COLNM)


def make_tmplt(url, title=None, parent=None, dst=None, src=None):
    """ make template for json that pushed to kafka

    :param url: url
    :param title: title
    :param parent: a list of dictionary(url and title) used to save the history of links
    :param dst: to link
    :param src: from link
    :return:
    """
    tmpl = {
        'url': make_wiki_link(url),
        'title': title,
        'parent': parent if parent else list(),
        'dst': dst,
    }

    tmpl['src'] = src if src else tmpl['url']
    return tmpl


def add_parents(old_prnts, url, title):
    """ append one more parents(url, title) to current list of parents

    :param old_prnts: current list of parents
    :param url: new url
    :param title: title url
    :return:
    """
    new_prnts = copy.deepcopy(old_prnts)
    new_prnts.append({
        'url': url,
        'title': title
    })
    return new_prnts


def push_en(entry):
    """ push entry to kafka

    :param entry: entry
    """
    prod.produce(entry)


def push(entries):
    """ push the entries to kafka using multiple threads

    :param entries: list of entries to push to kafka
    """
    with ft.ThreadPoolExecutor(max_workers=500) as executor:
        future_to_entry = {
            executor.submit(push_en, entry): entry
            for entry in entries}

        for no, future in enumerate(ft.as_completed(future_to_entry.keys())):
            # print('finish pushing. {}'.format(no))
            pass


def exists(bf, src, dst):
    """ using bloom filter to check if we already have records in memory
     save round trip time to db by avoiding call

    :param bf: bloom filter object
    :param src: from wiki path
    :param dst: to wiki path
    :return:
    """
    id = hash_str('{}||{}'.format(src, dst))
    if bf.lookup(id):
        print('bf check')
        return True
    else:
        if dbcon_oput.find_one({'_id': id}):
            bf.add_one(id)
            print('added to bf')
            return True
        else:
            return False


def update_db(entry, prs=None):
    """ make the entry for db ready and insert the path(parents list)

    :param entry: entry
    :param prs: parents list
    """
    # Hash the src and dst as key for db
    id = hash_str('{}||{}'.format(entry['src'], entry['dst']))

    if prs:
        entry['parent'] = add_parents(prs, entry['dst'], entry.get('title'))
    else:
        entry['parent'] = 'Reach Max depth'

    q = {'_id': id}
    dbcon_oput.update(q, {'$set': {'ans': entry['parent']}}, upsert=True)


def run():
    """ consume from kafka / retrieve links / compare / push new links back to kafka

    """
    # In memory bloom filter to prevent querying database
    bf = BloomFilter()

    for msg in cons.consume():
        found = False
        print(len(msg['parent']))

        # Check if we already processed this combination
        if exists(bf, msg['src'], msg['dst']):
            continue

        # add current url and title as parent for all children
        prs = add_parents(msg.get('parent'), msg.get('url'), msg.get('title'))

        # get all links in a page
        links_info = get_links(msg.get('url'))

        entries = []
        for link_inf in links_info:
            entry = make_tmplt(link_inf.get('url'), link_inf.get('title'), src=msg['src'],
                               dst=msg.get('dst'), parent=prs)

            entries.append(entry)

            # check if we reach the defined max depth
            if len(msg['parent']) == MAX_DEPTH:
                update_db(entry)
                break

            # found the path / update db
            if entry['url'] == entry['dst']:
                update_db(entry, prs)
                found = True
                break

        # Push entries to kafka using multiple threads
        if not found:
            push(entries)


if __name__ == '__main__':
    run()
