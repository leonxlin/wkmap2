"""Little script that should be equivalent to wikipedia2vec's save_text (word2vec format).

Using the wikipedia2vec CLI, save_text sometimes gives a UnicodeEncodeError for reasons 
that I could not determine. Perhaps it is related to Cython. This script is intended to
do be equivalent to `wikipedia2vec save_text --out-format=word2vec`. Example invocation:

python2 w2v_save_text.py $MODEL_FILE $OUTPUT_FILE

Note: the dependencies for this script are not included in requirements.txt since
Wikipedia2Vec only works with Python 2.

The code is basically copied from wikipedia2vec, which carries the following license:

Copyright 2017 Studio Ousia

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""


from wikipedia2vec import Wikipedia2Vec
import argparse
from tqdm import tqdm

arg_parser = argparse.ArgumentParser(description=
        "Little script that should be equivalent to wikipedia2vec's save_text "
        "(word2vec format).")
arg_parser.add_argument('model', type=str, 
                        help='File containing Wikipedia2Vec model (output of training).')
arg_parser.add_argument('output', type=str, 
                        help='File to write to.')
args = arg_parser.parse_args()


def run():
    w2v = Wikipedia2Vec.load(args.model)
    with open(args.output, 'wb') as f:
        f.write(('%d %d\n' % (len(w2v.dictionary), len(w2v.syn0[0]))).encode('utf-8'))

        for item in tqdm(sorted(w2v.dictionary, key=lambda o: o.doc_count, reverse=True)):
            vec_str = ' '.join('%.4f' % v for v in w2v.get_vector(item))
            if hasattr(item, 'text'):
                # `item` is an instance of Word
                text = item.text.replace('\t', ' ')
            else:
                text = 'ENTITY/' + item.title.replace('\t', ' ')

            text = text.replace(' ', '_')
            f.write(('%s %s\n' % (text, vec_str)).encode('utf-8'))



if __name__ == '__main__':
    run()
