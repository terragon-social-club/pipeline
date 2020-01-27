import { of, Observable, pipe, zip } from 'rxjs';
import { filter, take, tap } from 'rxjs/operators';
import { CouchDB, AuthorizationBehavior, CouchDBCredentials } from '@mkeen/rxcouch';

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

const ingressCommentsReactionsDb = new CouchDB(
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

const feedsDb = new CouchDB(
  {
    dbName: 'feeds',
    host: COUCH_HOST,
    port: parseInt(COUCH_PORT),
    ssl: COUCH_SSL === 'true' ? true : false,
    trackChanges: true
  },

  AuthorizationBehavior.cookie,
  credentials
);

const commentsDb = new CouchDB(
  {
    dbName: 'comments',
    host: COUCH_HOST,
    port: parseInt(COUCH_PORT),
    ssl: COUCH_SSL === 'true' ? true : false,
    trackChanges: true
  },

  AuthorizationBehavior.cookie,
  credentials
);

ingressCommentsDb.changes()
  .subscribe((commentIngress) => {
    commentsDb.doc(commentIngress.doc.conversation_id).pipe(take(1)).subscribe((conversation) => {
      conversation.comments.push({
        content: commentIngress.doc.content,
        sender_id: commentIngress.doc.sender_id,
        reactions: {
          loveface: 0,
          lol: 0,
          smile: 0,
          sad: 0,
          angry: 0
        } 
      });

      commentsDb.doc(conversation).pipe(take(1)).subscribe((response) => {
        ingressCommentsDb.delete(commentIngress.doc._id);
      });

    });

  });


let requestingComments = false;
let queuedCommentReactions: any = {
  loveface: [],
  lol: [],
  smile: [],
  sad: [],
  angry: []
}

ingressCommentsReactionsDb.changes()
  .subscribe((commentReactionIngress) => {
    commentsDb.doc(commentReactionIngress.doc.conversation_id)
    .pipe(take(1))
    .subscribe((conversationDocument) => {
      if(!requestingComments) {
        requestingComments = true;
        conversationDocument.comments[commentReactionIngress.doc.commentIndex].reactions[commentReactionIngress.doc.reactionType]++;
        if(queuedCommentReactions[commentReactionIngress.doc.reactionType][commentReactionIngress.doc.commentIndex]) {
          conversationDocument.comments[commentReactionIngress.doc.commentIndex].reactions[commentReactionIngress.doc.reactionType] = conversationDocument.comments[commentReactionIngress.doc.commentIndex].reactions[commentReactionIngress.doc.reactionType] + queuedCommentReactions[commentReactionIngress.doc.reactionType][commentReactionIngress.doc.commentIndex];
          queuedCommentReactions[commentReactionIngress.doc.reactionType][commentReactionIngress.doc.commentIndex] = null;
        }
        commentsDb.doc(conversationDocument)
          .pipe(take(1))
          .subscribe((response) => {
            console.log("sent reaction", response);
            requestingComments = false;
          });

      } else {
        if(queuedCommentReactions[commentReactionIngress.doc.reactionType][commentReactionIngress.doc.commentIndex]) {
          queuedCommentReactions[commentReactionIngress.doc.reactionType][commentReactionIngress.doc.commentIndex]++;
        } else {
          queuedCommentReactions[commentReactionIngress.doc.reactionType][commentReactionIngress.doc.commentIndex] = 1;
        }
      }
        
    });

  });

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