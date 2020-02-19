import { of, Observable, Observer, pipe, zip, queueScheduler, Subject, BehaviorSubject, concat } from 'rxjs';
import { filter, take, tap, takeUntil, last, buffer, map, mergeMap, catchError, finalize } from 'rxjs/operators';
import { CouchDB, AuthorizationBehavior, CouchDBCredentials, CouchSession } from '@mkeen/rxcouch';
import { CouchDBChanges, CouchDBDocument } from '@mkeen/rxcouch/dist/types';

const { COUCH_HOST, COUCH_PASS, COUCH_USER, COUCH_PORT, COUCH_SSL } = process.env;

const credentials: Observable<CouchDBCredentials> = of({
  username: COUCH_USER,
  password: COUCH_PASS
});

const couchSession: CouchSession = new CouchSession(
  AuthorizationBehavior.cookie,
  `${COUCH_SSL? 'https://' : 'http://'}${COUCH_HOST}:${COUCH_PORT}/_session`,
  credentials
);

const ingressCommentsDb = new CouchDB(
  {
    dbName: 'ingress_comments',
    host: COUCH_HOST,
    port: parseInt(COUCH_PORT),
    ssl: COUCH_SSL === 'true' ? true : false,
    trackChanges: false
  },

  couchSession
);

const commentsAggregateDb = new CouchDB(
  {
    dbName: 'comments',
    host: COUCH_HOST,
    port: parseInt(COUCH_PORT),
    ssl: COUCH_SSL === 'true' ? true : false,
    trackChanges: true
  },

  couchSession
);

const ingressCommentReactionsDb = new CouchDB(
  {
    dbName: 'ingress_reactions',
    host: COUCH_HOST,
    port: parseInt(COUCH_PORT),
    ssl: COUCH_SSL === 'true' ? true : false,
    trackChanges: false
  },

  couchSession
);

const feedsDb = new CouchDB(
  {
    dbName: 'feeds',
    host: COUCH_HOST,
    port: parseInt(COUCH_PORT),
    ssl: COUCH_SSL === 'true' ? true : false,
    trackChanges: true
  },

  couchSession
);

function changes(database: CouchDB, observer: Observer<any>, first_seq?: string) {
  const reconnect = new Subject();
  database.changes() // need to be able to pass `first_seq` here to start changes from our last known
    .pipe(takeUntil(reconnect))
    .subscribe((change) => {
      observer.next(change);
    }, (err) => {
      //console.log("Error occurred")
    }, () => {
      changes(database, observer);
      reconnect.next(true);
    });

}

const ingressComments = Observable.create((observer: Observer<CouchDBChanges>) => changes(ingressCommentsDb, observer));
const ingressCommentReactions = Observable.create((observer: Observer<CouchDBChanges>) => changes(ingressCommentReactionsDb, observer));

const savingCommentsBatch = new BehaviorSubject(false);
ingressComments
  .pipe(buffer(savingCommentsBatch))
  .subscribe((changes: CouchDBChanges[]) => {
    if (changes.length) {
      //console.log("got comment chanfges", changes)
      const newMessages: any = {};
      changes.forEach(({ doc }) => {
        const { content, conversation_id, sender_id } = doc;

        if(!newMessages[conversation_id]) {
          newMessages[conversation_id] = [];
        }

        //console.log(doc, "changed")

        newMessages[conversation_id].push({
          reactions: {
            "loveface": 0,
            "lol": 0,
            "smile": 0,
            "sad": 0,
            "angry": 0
          },
          content,
          sender_id
        });

      });

      const conversation_get_observables: Observable<CouchDBDocument>[] = [];
      const conversation_update_observables: Observable<CouchDBDocument>[] = [];

      Object.keys(newMessages).forEach((conversation_id) => {
        conversation_get_observables.push(
          commentsAggregateDb.doc(conversation_id).pipe(take(1), mergeMap((aggregateDocument) => {
            if(!aggregateDocument.comments) {
              aggregateDocument.comments = [];
            }

            aggregateDocument.comments = aggregateDocument.comments.concat(newMessages[aggregateDocument._id]);

            return commentsAggregateDb.doc(aggregateDocument).pipe(take(1));
          }))

        )

      });

      zip(...conversation_get_observables)
        .pipe(take(1))
        .subscribe((_existing) => {
          savingCommentsBatch.next(true);
        });

    } else {
      setTimeout(() => {
        savingCommentsBatch.next(true);
      }, 1);

    }

  });

const savingReactionsBatch = new BehaviorSubject(false);
ingressCommentReactions
  .pipe(buffer(savingReactionsBatch))
  .subscribe((changes: CouchDBChanges[]) => {
    if (changes.length) {
      const queuedCommentReactions: any = {}
      const commentIndexes: number[] = [];

      changes.forEach(({ doc }) => {
        if (!queuedCommentReactions[doc.conversation_id]) {
          queuedCommentReactions[doc.conversation_id] = {
            loveface: [],
            lol: [],
            smile: [],
            sad: [],
            angry: []
          };
        }

        if(!queuedCommentReactions[doc.conversation_id][doc.reactionType][doc.commentIndex]) {
          queuedCommentReactions[doc.conversation_id][doc.reactionType][doc.commentIndex] = 1;
        } else {
          queuedCommentReactions[doc.conversation_id][doc.reactionType][doc.commentIndex]++;
        }

        commentIndexes.push(doc.commentIndex);
      });

      const conversation_get_observables: Observable<CouchDBDocument>[] = [];
      const conversation_update_observables: Observable<CouchDBDocument>[] = [];

      Object.keys(queuedCommentReactions).forEach((conversation_id) => {
        conversation_get_observables.push(
          commentsAggregateDb.doc(conversation_id).pipe(take(1), mergeMap((currentConvo: any) => {
            commentIndexes.forEach((commentIndex) => {
              Object.keys(currentConvo.comments[commentIndex].reactions).forEach((reactionType) => {
                //console.log(queuedCommentReactions[conversation_id][reactionType][commentIndex])
                if (queuedCommentReactions[conversation_id][reactionType][commentIndex] !== undefined) {
                  currentConvo.comments[commentIndex].reactions[reactionType] = currentConvo.comments[commentIndex].reactions[reactionType] + queuedCommentReactions[conversation_id][reactionType][commentIndex]
                }

              });

            });

            return commentsAggregateDb.doc(currentConvo).pipe(take(1));
          }))

        );

      });

      concat(...conversation_get_observables)
        .pipe(take(1), finalize(() => {
          savingReactionsBatch.next(true);
        }))
        .subscribe((existingAggregateDocuments) => {
          //console.log("docs written");
        });

    } else {
      setTimeout(() => {
        savingReactionsBatch.next(true);
      }, 1);

    }

  });

feedsDb.doc('hot').subscribe(hotFeed => {
  hotFeed.links.map((link: any) => {
    commentsAggregateDb.doc({_id: link.slug, title: link.title, url: link.url, badges: link.badges})
    .pipe(take(1))
    .subscribe((doc) => {
      console.log("created comments doc");
    }, (err) => {
      console.log("doc exists")
    });

  });

});

process.stdin.resume();
