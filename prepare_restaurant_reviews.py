import json
from tqdm import tqdm
import spacy
import argparse
import os
from joblib import Parallel, delayed
import logging
import sys

def set_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

logging = set_logger("prepare_restaurante_reviews")

def chunker(iterable, total_length, chunksize):
    return (iterable[pos: pos + chunksize] for pos in range(0, total_length, chunksize))

def flatten(list_of_lists):
    "Flatten a list of lists to a combined list"
    return [item for sublist in list_of_lists for item in sublist]


def sentence_segment_filter_docs_parallel(texts, chunksize=1000):
    executor = Parallel(n_jobs=7, backend='multiprocessing', prefer="processes")
    do = delayed(sentence_segment_filter_docs)
    tasks = (do(chunk) for chunk in chunker(texts, len(texts), chunksize=chunksize))
    result = executor(tasks)
    return flatten(result)


parser = argparse.ArgumentParser(description='Generate finetuning corpus for restaurants.')

parser.add_argument('--large',
                    action='store_true',
                    help='export large corpus (10 mio), default is 1 mio')
args = parser.parse_args()

max_sentences = int(10e5)
review_limit = int(150000)
if args.large:
    review_limit = int(1500000)  # for 10 Mio Corpus
    max_sentences = int(10e6)  # for 10 Mio corpus

nlp = spacy.load('en_core_web_sm')
nlp.create_pipe('sentencizer')
nlp.add_pipe('sentencizer')
fn = 'data/raw/review.json'
reviews = []

# 4 million reviews to generate about minimum 10 mio sentences
with open(fn) as data_file:
    counter = 0
    for line in data_file:
        counter += 1
        reviews.append(json.loads(line)['text'])
        if counter == review_limit:
            break


# get sentence segemented review with #sentences > 2
def sentence_segment_filter_docs(doc_array):
    sentences = []

    for doc in nlp.pipe(doc_array, disable=['parser','ner'], batch_size=1000): # n_threads does not work
        sentences.append([sent.text.strip() for sent in doc.sents])
    logging.info("pid={}, sentences={}".format(os.getpid(),len(sentences)))
    return sentences


logging.info(f'Found {len(reviews)} restaurant reviews')
logging.info(f'Tokenizing Restaurant Reviews...')

sentences = sentence_segment_filter_docs_parallel(reviews)
nr_sents = sum([len(s) for s in sentences])
logging.info(f'Segmented {nr_sents} restaurant sentences')

# Save to file
fn_out = f'data/transformed/restaurant_corpus_{max_sentences}.txt'
with open(fn_out, "w") as f:
    sent_count = 0
    for sents in tqdm(sentences):
        if sent_count%1000==0:
            logging.info("pid={}, processed {}/{} sentences".format(os.getpid(), sent_count,len(sentences)))
        real_sents = []
        for s in sents:
            x = s.replace(' ', '').replace('\n', '').replace('\u200d', '').replace('\u200b', '')
            if x != '':
                if s=="By far the best Avacado bread I have ever had.":
                    logging.info(sents)
                    pass
                real_sents.append(s.replace('\n', '').replace('\u200d', '').replace('\u200b', ''))
        if len(real_sents) >= 2:
            sent_count += len(real_sents)
            str_to_write = "\n" + "\n".join(real_sents) + "\n"
            f.write(str_to_write)

        if sent_count >= max_sentences:
            break

logging.info(f'Done writing to {fn_out}')
