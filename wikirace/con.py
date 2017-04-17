from wikirace.configuration import INGESTION_TOPIC, OUTPUT_COLNM
from wikirace.reader import make_tmplt, get_links
from wikirace.utils.bfilter import BloomFilter
from wikirace.utils.kfkpywrapper import KfkConsumer, KfkProducer
from wikirace.utils.mongo import mongo_connect
from wikirace.utils.string import hash_str
import copy
import concurrent.futures as ft

cons = KfkConsumer(INGESTION_TOPIC, 'con')
# cons = KfkConsumer(INGESTION_TOPIC, 'con', set_offset_to_end=True)
prod = KfkProducer(INGESTION_TOPIC)
dbcon_oput = mongo_connect(col_nm=OUTPUT_COLNM)


def add_parents(old_prnts, url, title):
    new_prnts = copy.deepcopy(old_prnts)
    new_prnts.append({
        'url': url,
        'title': title
    })
    return new_prnts


def push_en(entry):
    prod.produce(entry)


def push(entries):
    with ft.ThreadPoolExecutor(max_workers=500) as executor:
        future_to_entry = {
            executor.submit(push_en, entry): entry
            for entry in entries}

        for no, future in enumerate(ft.as_completed(future_to_entry.keys())):
            # print('finish pushing. {}'.format(no))
            pass


def exists(bf, src, dst):
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


def consume():
    bf = BloomFilter()
    for msg in cons.consume():
        print(len(msg['parent']))

        if exists(bf, msg['src'], msg['dst']):
            continue

        found = False

        prs = add_parents(msg.get('parent'), msg.get('url'), msg.get('title'))

        links_info = get_links(msg.get('url'))

        entries = []
        for link_inf in links_info:
            entry = make_tmplt(link_inf.get('url'), link_inf.get('title'), src=msg['src'],
                               dst=msg.get('dst'), parent=prs)

            entries.append(entry)
            if entry['url'] == entry['dst']:
                id = hash_str('{}||{}'.format(entry['src'], entry['dst']))
                entry['parent'] = add_parents(prs, entry['dst'], entry.get('title'))
                en = {'_id': id}
                dbcon_oput.update(en, {'$set': {'ans': entry['parent']}}, upsert=True)
                found = True
                break

        if found:
            continue
        else:
            push(entries)
            # for entry in entries:
            #     prod.produce(entry)

            # entries['e'].append(entry)
            # prod.produce(entry)
            # yield entry
            #     pass


if __name__ == '__main__':
    consume()
    # for msg in cons.consume():
    #     print(msg)
