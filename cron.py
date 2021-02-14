import praw
from data import *
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk
import config
import mysql.connector
nltk.download('vader_lexicon')

reddit = praw.Reddit(
    user_agent="Comment Extraction",
    client_id="qwGA2WkSA9pRyQ",
    client_secret="joYl7_cCOwE2ezzmHIt55wOuKCYZCQ"
)

cnx = mysql.connector.connect(user=config.DB_USER, password=config.DB_PASS,
                              unix_socket=config.UNIX_SOCKET,
                              database=config.DB_NAME)
cursor = cnx.cursor()

# cleaning database
cursor.execute("""
        DELETE FROM results
    """)
cnx.commit()

'''############################################################################'''
# set the program parameters
subs = {'wallstreetbets', 'stocks'}
#subs = ['wallstreetbets', 'stocks', 'investing', 'stockmarket']     # sub-reddit to search
#post_flairs = {'Daily Discussion', 'Weekend Discussion', 'Discussion'}    # posts flairs to search || None flair is automatically considered
post_flairs = {'Daily Discussion'} 
goodAuth = {'AutoModerator'}   # authors whom comments are allowed more than once
uniqueCmt = True                # allow one comment per author per symbol
ignoreAuthP = {'example'}       # authors to ignore for posts 
ignoreAuthC = {'example'}       # authors to ignore for comment 
upvoteRatio = 0.70         # upvote ratio for post to be considered, 0.70 = 70%
ups = 20       # define # of upvotes, post is considered if upvotes exceed this #
limit = 10      # define the limit, comments 'replace more' limit
upvotes = 2     # define # of upvotes, comment is considered if upvotes exceed this #
picks = 10     # define # of picks here, prints as "Top ## picks are:"
picks_ayz = 10   # define # of picks for sentiment analysis
'''############################################################################'''

posts, count, c_analyzed, tickers, titles, a_comments = 0, 0, 0, {}, [], {}
cmt_auth = {}

# start_time = time.time()
for sub in subs:
    subreddit = reddit.subreddit(sub)
    hot_python = subreddit.hot()    # sorting posts by hot
    # Extracting comments, symbols from subreddit
    for submission in hot_python:
        
        flair = submission.link_flair_text 
        
        try: author = submission.author.name
        except: pass
        
        # checking: post upvote ratio # of upvotes, post flair, and author 
        if submission.upvote_ratio >= upvoteRatio and submission.ups > ups and (flair in post_flairs or flair is None) and author not in ignoreAuthP:   
            submission.comment_sort = 'new'     
            comments = submission.comments
            titles.append(submission.title)
            posts += 1
            submission.comments.replace_more(limit=limit)   
            for comment in comments:
                # try except for deleted account?
                try: auth = comment.author.name
                except: pass
                c_analyzed += 1
                
                # checking: comment upvotes and author
                if comment.score > upvotes and auth not in ignoreAuthC:      
                    split = comment.body.split(" ")
                    for word in split:
                        word = word.replace("$", "")        
                        # upper = ticker, length of ticker <= 5, excluded words,                     
                        if word.isupper() and len(word) <= 5 and word not in blacklist and word in us:
                            
                            # unique comments, try/except for key errors
                            if uniqueCmt and auth not in goodAuth:
                                try: 
                                    if auth in cmt_auth[word]: break
                                except: pass
                                
                            # counting tickers
                            if word in tickers:
                                tickers[word] += 1
                                a_comments[word].append(comment.body)
                                cmt_auth[word].append(auth)
                                count += 1
                            else:                               
                                tickers[word] = 1
                                cmt_auth[word] = [auth]
                                a_comments[word] = [comment.body]
                                count += 1

    # sorts the dictionary/
    stocks_per_subs = dict(sorted(tickers.items(), key=lambda item: item[1], reverse = True)) #nume stock si valoarea sa
    top_picks_per_subs = list(stocks_per_subs.keys())[0:picks] #doar nume stocks
    # print("Stocks for subreddit: " + sub + "\n")

    for i in top_picks_per_subs:
        # print(i, ' : ', stocks_per_subs[i])

        cursor.execute("""
            INSERT INTO results (stock, mentions, source)
            VALUES (%s, %s, %s)
        """, (i, stocks_per_subs[i], sub))

        cnx.commit()

    # Applying Sentiment Analysis
    scores, s = {}, {}
    vader = SentimentIntensityAnalyzer()
    # adding custom words from data.py 
    vader.lexicon.update(new_words)

    picks_sentiment = list(stocks_per_subs.keys())[0:picks_ayz]
    for symbol in picks_sentiment:
        stock_comments = a_comments[symbol]
        for cmnt in stock_comments:
            score = vader.polarity_scores(cmnt)
            if symbol in s:
                s[symbol][cmnt] = score
            else:
                s[symbol] = {cmnt:score}      
            if symbol in scores:
                for key, _ in score.items():
                    scores[symbol][key] += score[key]
            else:
                scores[symbol] = score
                
        # calculating avg.
        for key in score:
            scores[symbol][key] = scores[symbol][key] / stocks_per_subs[symbol]
            scores[symbol][key]  = "{pol:.3f}".format(pol=scores[symbol][key])

    #Print values from scores
    for stock, value in scores.items():
    #    print(stock, '--')

        cursor.execute("""
            UPDATE results SET bearish=%s, neutral=%s, bullish=%s, total=%s WHERE stock=%s
        """, (value['neg'], value['neu'], value['pos'], value['compound'], stock))

        cnx.commit()

cursor.close()
cnx.close()

print('Done!')
