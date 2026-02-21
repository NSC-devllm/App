-- PostgreSQL 18+: users.user_id 기본값을 DB native uuidv7()로 설정한다.
ALTER TABLE users
  ALTER COLUMN user_id SET DEFAULT uuidv7();
