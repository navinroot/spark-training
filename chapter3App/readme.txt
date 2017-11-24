this project takes json file as input .
and process it.

-------------------------------------------------------------------
args[0] json sample:
{
  "org": {
    "avatar_url": "https://avatars.githubusercontent.com/u/10965476?",
    "url": "https://api.github.com/orgs/Early-Modern-OCR",
    "gravatar_id": "",
    "login": "Early-Modern-OCR",
    "id": 10965476
  },
  "created_at": "2015-03-01T00:00:00Z",
  "public": true,
  "payload": {
    "pusher_type": "user",
    "description": "",
    "master_branch": "master",
    "ref_type": "branch",
    "ref": "development"
  },
  "repo": {
    "url": "https://api.github.com/repos/Early-Modern-OCR/emop-dashboard",
    "name": "Early-Modern-OCR/emop-dashboard",
    "id": 23934080
  },
  "actor": {
    "avatar_url": "https://avatars.githubusercontent.com/u/739622?",
    "url": "https://api.github.com/users/treydock",
    "gravatar_id": "",
    "login": "treydock",
    "id": 739622
  },
  "type": "CreateEvent",
  "id": "2614896652"
}

------------------------------------------------------------------------------
args[1]- employee list
args[2]- output location (location/dir should be new not already exist)
args[3]- output format