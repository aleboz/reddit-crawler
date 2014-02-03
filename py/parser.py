import json
import pprint
import sys

# Extract post from a post+comments display
def extract_post(blob):
    data = blob[0]['data']['children'][0]['data']
    data[u'kind'] = u't3'
    return data

# Extract comments from a post+comments display
def extract_post_comments(blob):
    comment_list = []
    for subblob in blob:
        wrapper = subblob['data']['children']
        for child in wrapper:
            if child['kind'] != 't1':
                continue
            comment = dict()
            for key in child['data']:
                if key == 'replies' and child['data']['replies'] != '':
                    replies = [ child['data']['replies'] ]
                    comments = extract_post_comments(replies)
                    comment_list.extend(comments)
                else:
                    comment[key] = child['data'][key]
            comment[u'kind'] = u't1'
            comment_list.append(comment)
    return comment_list

# Extract individual comments that need to be requested from a post+comments display
def extract_post_comments_missing(blob):
    comment_list = []
    for subblob in blob:
        wrapper = subblob['data']['children']
        for child in wrapper:
            if child['kind'] == 'more':
                comment_list.extend(child['data']['children'])
            elif child['kind'] == 't1':
                if child['data']['replies'] != '':
                    replies = [ child['data']['replies'] ]
                    comments = extract_post_comments_missing(replies)
                    comment_list.extend(comments)
    return comment_list

# Extract posts from a listing
def extract_listing_elements(blob):
    element_list = []
    for child in blob['data']['children']:
        data = child['data']
        data[u'kind'] = child['kind']
        element_list.append(data)
    return element_list

# Extract navigation elements from a listing
def extract_listing_nav(blob):
    nav = dict()
    nav[u'after'] = blob['data']['after']
    nav[u'before'] = blob['data']['before']
    return nav

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'Need one of \'post\', \'comments\', \'missing\', \'author\''
        quit()
    if sys.argv[1] == 'post':
        fp = open('json/testpost.json')
        post = extract_post(json.load(fp))
        fp.close()
        pprint.pprint(post)
    elif sys.argv[1] == 'comments':
        fp = open('json/testpost.json')
        comments = extract_post_comments(json.load(fp))
        fp.close()
        pprint.pprint(comments)
    elif sys.argv[1] == 'missing':
        fp = open('json/testpost.json')
        comments = extract_post_comments_missing(json.load(fp))
        fp.close()
        pprint.pprint(comments)
    elif sys.argv[1] == 'author':
        fp = open('json/testlisting.json')
        listing = extract_listing_elements(json.load(fp))
        fp.close()
        pprint.pprint(listing)
    elif sys.argv[1] == 'nav':
        fp = open('json/testlisting.json')
        listing = extract_listing_nav(json.load(fp))
        fp.close()
        pprint.pprint(listing)
