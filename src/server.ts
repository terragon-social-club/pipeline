import { of, Observable, Observer, pipe, zip, queueScheduler, Subject, BehaviorSubject } from 'rxjs';
import { filter, take, tap, takeUntil, last, buffer } from 'rxjs/operators';
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
    trackChanges: false
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
    trackChanges: false
  },

  couchSession
);

function changes(database: CouchDB, observer: Observer<any>, first_seq?: string) {
  const reconnect = new Subject();
  database.changes() // need to be able to pass `first_seq` here to start changes from our last known
    .pipe(takeUntil(reconnect))
    .subscribe((change) => {
      console.log("got change", change)
      observer.next(change);
    }, err => err, () => {
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
      console.log("got comment chanfges", changes)
      const newMessages: any = {};
      changes.forEach(({ doc }) => {
        const { content, conversation_id, sender_id } = doc;

        if(!newMessages[conversation_id]) {
          newMessages[conversation_id] = [];
        }

        console.log(doc, "changed")

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
        console.log("new ones")
        conversation_get_observables.push(commentsAggregateDb.doc(conversation_id))
      });

      zip(...conversation_get_observables)
        .pipe(take(1))
        .subscribe((existingAggregateDocuments) => {
          existingAggregateDocuments.forEach((aggregateDocument) => {
            if(!aggregateDocument.comments) {
              aggregateDocument.comments = [];
            }

            aggregateDocument.comments = aggregateDocument.comments.concat(newMessages[aggregateDocument._id]);
            conversation_update_observables.push(commentsAggregateDb.doc(aggregateDocument))
          });

          zip(...conversation_update_observables)
            .pipe(take(1))
            .subscribe((_done) => {
              savingCommentsBatch.next(true);
            });

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
        conversation_get_observables.push(commentsAggregateDb.doc(conversation_id))
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

            conversation_update_observables.push(commentsAggregateDb.doc(aggregateDocument));
          });

          console.log(conversation_update_observables, "saving i think");

          zip(...conversation_update_observables)
            .pipe(take(1))
            .subscribe((_done) => {
              savingReactionsBatch.next(true);
            });

        });

    } else {
      savingReactionsBatch.next(true);
    }

  });

process.stdin.resume();
