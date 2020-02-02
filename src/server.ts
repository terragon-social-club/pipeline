import { of, Observable, Observer, pipe, zip, queueScheduler, Subject, BehaviorSubject } from 'rxjs';
import { filter, take, tap, takeUntil, last, buffer } from 'rxjs/operators';
import { CouchDB, AuthorizationBehavior, CouchDBCredentials } from '@mkeen/rxcouch';
import { CouchDBChanges, CouchDBDocument } from '@mkeen/rxcouch/dist/types';

const { COUCH_HOST, COUCH_PASS, COUCH_USER, COUCH_PORT, COUCH_SSL } = process.env;

const credentials: Observable<CouchDBCredentials> = of({
  username: COUCH_USER,
  password: COUCH_PASS
});

const ingressCommentsDb = new CouchDB(
  {
    dbName: 'ingress_comments',
    host: COUCH_HOST,
    port: parseInt(COUCH_PORT),
    ssl: COUCH_SSL === 'true' ? true : false,
    trackChanges: false
  },

  AuthorizationBehavior.cookie,
  credentials
);

const commentsAgrregateDb = new CouchDB(
  {
    dbName: 'comments',
    host: COUCH_HOST,
    port: parseInt(COUCH_PORT),
    ssl: COUCH_SSL === 'true' ? true : false,
    trackChanges: false
  },

  AuthorizationBehavior.cookie,
  credentials
);

const ingressCommentReactionsDb = new CouchDB(
  {
    dbName: 'ingress_reactions',
    host: COUCH_HOST,
    port: parseInt(COUCH_PORT),
    ssl: COUCH_SSL === 'true' ? true : false,
    trackChanges: false
  },

  AuthorizationBehavior.cookie,
  credentials
);

function changes(database: CouchDB, observer: Observer<any>, first_seq?: string) {
  const reconnect = new Subject();
  database.changes() // need to be able to pass `first_seq` here to start changes from our last known
    .pipe(takeUntil(reconnect))
    .subscribe((change) => {
      observer.next(change);
    }, (err) => {
      const last_seq = err.last_seq;
      if(err.errorCode == 200) {
        changes(database, observer, last_seq);
        reconnect.next(true);
      } else {
        throw err;
      }

    });

}

const ingressComments = Observable.create((observer: Observer<CouchDBChanges>) => changes(ingressCommentsDb, observer));
const ingressCommentReactions = Observable.create((observer: Observer<CouchDBChanges>) => changes(ingressCommentReactionsDb, observer));

const savingBatch = new BehaviorSubject(false);
ingressComments
  .pipe(buffer(savingBatch))
  .subscribe((changes: CouchDBChanges[]) => {
    if (changes.length) {
      const newMessages: any = {};
      changes.forEach(({ doc }) => {
        const { content, conversation_id, sender_id } = doc;

        if(!newMessages[conversation_id]) {
          newMessages[conversation_id] = [];
        }

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
        conversation_get_observables.push(commentsAgrregateDb.doc(conversation_id))
      });

      zip(...conversation_get_observables)
        .pipe(take(1))
        .subscribe((existingAggregateDocuments) => {
          existingAggregateDocuments.forEach((aggregateDocument) => {
            aggregateDocument.comments = aggregateDocument.comments.concat(newMessages[aggregateDocument._id]);
            conversation_update_observables.push(commentsAgrregateDb.doc(aggregateDocument))
          });

          zip(...conversation_update_observables)
            .pipe(take(1))
            .subscribe((_done) => {
              savingBatch.next(true);
            });

        });

    } else {
      setTimeout(() => {
        savingBatch.next(true);
      }, 1);

    }

  });

const savingReactionsBatch = new BehaviorSubject(false);
ingressCommentReactions
  .pipe(buffer(savingReactionsBatch))
  .subscribe((changes: CouchDBChanges[]) => {
    if (changes.length) {
      const queuedCommentReactions: any = {}

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

      });

      const conversation_get_observables: Observable<CouchDBDocument>[] = [];
      const conversation_update_observables: Observable<CouchDBDocument>[] = [];

      Object.keys(queuedCommentReactions).forEach((conversation_id) => {
        conversation_get_observables.push(commentsAgrregateDb.doc(conversation_id))
      });

      zip(...conversation_get_observables)
        .pipe(take(1))
        .subscribe((existingAggregateDocuments) => {
          existingAggregateDocuments.forEach((aggregateDocument) => {
            Object.keys(queuedCommentReactions[aggregateDocument._id]).forEach((reactionType: string) => {
              Object.keys(queuedCommentReactions[aggregateDocument._id][reactionType]).forEach((commentIndex: string) => {
                aggregateDocument.comments[commentIndex].reactions[reactionType] = aggregateDocument.comments[commentIndex].reactions[reactionType] + queuedCommentReactions[aggregateDocument._id][reactionType][commentIndex];
              });

            });

            conversation_update_observables.push(commentsAgrregateDb.doc(aggregateDocument));
          });

          console.log(conversation_update_observables);

          zip(...conversation_update_observables)
            .pipe(take(1))
            .subscribe((_done) => {
              savingReactionsBatch.next(true);
            });

        });

    } else {
      setTimeout(() => {
        savingReactionsBatch.next(true);
      }, 1);

    }

  });


/*ingressCommentsReactionsDb.changes()
  .subscribe((commentReactionIngress) => {
    console.log(commentReactionIngress)
  });*/

//ingressCommentsDb.doc("d2ea3d826856bafee511524cc001823f").subscribe((a) => console.log("changed", a))

/*feedsDb.doc('hot')
  .subscribe((hotFeed => {
    hotFeed.links.map((link: any) => {
      commentsDb.doc({_id: link.slug, title: link.title, url: link.url, badges: link.badges})
      .pipe(take(1))
      .subscribe((doc) => {
        console.log("doc exists");
      }, (err) => {
        console.log("Errrrr")
      }, () => {
        console.log("ended")
      });
    });

  }));*/

process.stdin.resume();