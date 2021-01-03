// couchstore file format docs: https://github.com/couchbaselabs/couchstore/wiki/Format
// couchdb data structures: https://github.com/apache/couchdb-couch/blob/master/include/couch_db.hrl
// couchdb impl info: https://horicky.blogspot.com/2008/10/couchdb-implementation.html
// pouchdb-next: https://github.com/pouchdb/pouchdb/issues/3775 / https://github.com/pouchdb/pouchdb/pull/4984
// https://github.com/kxepal/replipy

var allDocs_result = {
  "total_rows": 1,
  "offset": 0,
  "rows": [
    {
      "id": "test",
      "key": "test",
      "value": {
        "rev": "2-cc741a6956c9d99895a350e45954b8b3"
      }
    }
  ]
}


var changes_style_all_docs_result = {
  "results": [
    {
      "id": "test",
      "changes": [
        {
          "rev": "2-cc741a6956c9d99895a350e45954b8b3"
        },
        {
          "rev": "2-ab133135102dd4a48350ec69f4eeb23e"
        }
      ],
      "seq": 3
    }
  ],
  "last_seq": 3
};

var by_seq = {
  1: {"test": 123, "_doc_id_rev": "test::1-cb133135102dd4a48350ec69f4eeb23e"},
  2: {"test": 456, "_doc_id_rev": "test::2-cc741a6956c9d99895a350e45954b8b3"},
  3: {"test": 789, "_doc_id_rev": "test::2-ab133135102dd4a48350ec69f4eeb23e"}
};

var doc_store = {
  'test': {
    // in reality, 'data' is stringified json
    "data": {
      "id": "test",
      "rev": "2-ab133135102dd4a48350ec69f4eeb23e",
      "rev_tree": [
        {
          "pos": 1,
          "ids": [
            "cb133135102dd4a48350ec69f4eeb23e",
            {
              "status": "available"
            },
            [
              [
                "cc741a6956c9d99895a350e45954b8b3",
                {
                  "status": "available"
                },
                []
              ]
            ]
          ]
        },
        {
          "pos": 2,
          "ids": [
            "ab133135102dd4a48350ec69f4eeb23e",
            {
              "status": "available"
            },
            []
          ]
        }
      ],
      "winningRev": "2-cc741a6956c9d99895a350e45954b8b3",
      "deleted": false,
      "seq": 3
    },
    "winningRev": "2-cc741a6956c9d99895a350e45954b8b3",
    "deletedOrLocal": "0",
    "seq": 3,
    "id": "test"
  }
}

var more_complex = {
  "id": "test",
  "rev": "3-7af3de3529794feb778d330e81a45b31",
  "deleted": false,
  "rev_tree": [
    {
      "pos": 1,
      "ids": [
        "x",
        {
          "status": "available"
        },
        []
      ]
    },
    {
      "pos": 2,
      "ids": [
        "x",
        {
          "status": "available"
        },
        []
      ]
    },
    {
      "pos": 2,
      "ids": [
        "y",
        {
          "status": "available"
        },
        [
          [
            "7af3de3529794feb778d330e81a45b31",
            {
              "status": "available",
              "deleted": true
            },
            []
          ]
        ]
      ]
    },
    {
      "pos": 2,
      "ids": [
        "a",
        {
          "status": "available"
        },
        []
      ]
    }
  ],
  "winningRev": "2-x",
  "seq": 5
};
