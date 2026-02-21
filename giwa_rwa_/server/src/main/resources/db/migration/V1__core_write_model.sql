-- users: 서비스 사용자와 오프체인 컴플라이언스(KYC/심사) 상태를 관리하는 기준 테이블
CREATE TABLE IF NOT EXISTS users (
  -- 사용자 고유 ID(UUID, DB 기본값 uuidv7()).
  -- 예시: '7e0afdb6-6a88-4c28-89eb-9d4f4d0c79fd'
  user_id UUID PRIMARY KEY DEFAULT uuidv7(),
  -- 사용자 생성 시각(UTC 타임존 포함).
  -- 예시: '2026-02-17T10:15:30Z'
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- 컴플라이언스 상태.
  -- 예시: 'PENDING' | 'APPROVED' | 'REVOKED'
  compliance_status TEXT NOT NULL,
  -- 컴플라이언스 상태가 마지막으로 변경된 시각.
  -- 예시: '2026-02-17T10:20:00Z'
  compliance_updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- wallets: 사용자 지갑 주소와 암호화된 개인키 정보를 저장하는 1:1 테이블
CREATE TABLE IF NOT EXISTS wallets (
  -- users.user_id를 그대로 사용하는 1:1 키(FK + PK).
  -- 예시: '7e0afdb6-6a88-4c28-89eb-9d4f4d0c79fd'
  user_id UUID PRIMARY KEY REFERENCES users(user_id),
  -- 온체인 지갑 주소(0x 포함 42자), 전체 시스템에서 유일해야 함.
  -- 예시: '0x9f7c84f48a4df5f6cfe0f5ce1f5f11f6b9b38c58'
  address VARCHAR(42) NOT NULL UNIQUE,
  -- 서버가 암호화해 저장한 개인키 바이트.
  -- 예시: '\x8f11ab...'(BYTEA Hex 표현)
  encrypted_privkey BYTEA NOT NULL,
  -- 개인키 암호화 포맷/키 버전(키 로테이션 대응).
  -- 예시: 1
  enc_version INT NOT NULL,
  -- 지갑 레코드 생성 시각.
  -- 예시: '2026-02-17T10:15:31Z'
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- complexes: 단지(아파트) 공공데이터 원본과 요약 필드를 저장하는 테이블
CREATE TABLE IF NOT EXISTS complexes (
  -- K-apt 단지 코드(도메인 식별자).
  -- 예시: 'A10027831'
  kapt_code TEXT PRIMARY KEY,
  -- 단지명.
  -- 예시: '래미안아파트'
  kapt_name TEXT,
  -- 지번 주소.
  -- 예시: '서울특별시 강남구 대치동 123'
  kapt_addr TEXT,
  -- 도로명 주소.
  -- 예시: '서울특별시 강남구 테헤란로 123'
  doro_juso TEXT,
  -- 세대수(호수).
  -- 예시: 842
  ho_cnt INT,
  -- 외부에서 수집한 단지 원본 JSON 전체.
  -- 예시: '{"kaptCode":"A10027831","hoCnt":842,...}'
  raw_json JSONB NOT NULL,
  -- 마지막 수집 시각.
  -- 예시: '2026-02-17T09:00:00Z'
  fetched_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- classes: 단지 내 타입(평형/군) 단위의 토큰화 대상 정보를 저장하는 테이블
CREATE TABLE IF NOT EXISTS classes (
  -- 클래스 고유 ID(일반적으로 'kapt_code:class_key'를 keccak 해시한 값).
  -- 예시: '0x7c0c8b2f0f2b8f9d4e15a7e8df31ad0f6a9f7d6f5c4b3a291817161514131211'
  class_id TEXT PRIMARY KEY,
  -- 소속 단지 코드(FK -> complexes.kapt_code).
  -- 예시: 'A10027831'
  kapt_code TEXT NOT NULL REFERENCES complexes(kapt_code),
  -- 클래스 구분 키(평형 그룹).
  -- 예시: 'MPAREA_85'
  class_key TEXT NOT NULL,
  -- 해당 클래스의 총 유닛 수.
  -- 예시: 120
  unit_count INT NOT NULL,
  -- 클래스 상태.
  -- 예시: 'IMPORTED' | 'TOKENIZED'
  status TEXT NOT NULL,
  -- 클래스 메타데이터 문서 해시(토큰화 시점 계산값).
  -- 예시: '0x3f5a2b1c...'
  doc_hash TEXT,
  -- 온체인에서 클래스에 할당된 base token id.
  -- 예시: 1000001
  base_token_id NUMERIC(78,0),
  -- 온체인 발행 기준 시각.
  -- 예시: '2026-02-17T11:00:00Z'
  issued_at TIMESTAMPTZ,
  -- 클래스 레코드 최초 생성 시각.
  -- 예시: '2026-02-17T09:01:00Z'
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- 클래스 레코드 최종 수정 시각.
  -- 예시: '2026-02-17T11:00:05Z'
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- 같은 단지 내 동일 class_key 중복 방지.
  UNIQUE (kapt_code, class_key)
);

-- units: 클래스 내부의 개별 유닛(분할 단위) 상태와 토큰 ID를 저장하는 테이블
CREATE TABLE IF NOT EXISTS units (
  -- 유닛 고유 ID(일반적으로 'kapt_code:class_key:00001' 형식).
  -- 예시: 'A10027831:MPAREA_85:00001'
  unit_id TEXT PRIMARY KEY,
  -- 소속 클래스 ID(FK -> classes.class_id).
  -- 예시: '0x7c0c8b2f0f2b8f9d4e15a7e8df31ad0f6a9f7d6f5c4b3a291817161514131211'
  class_id TEXT NOT NULL REFERENCES classes(class_id),
  -- 클래스 내 순번(1부터 시작).
  -- 예시: 1
  unit_no INT NOT NULL,
  -- 해당 유닛에 매핑된 ERC-1155 token id(base_token_id + unit offset).
  -- 예시: 1000001
  token_id NUMERIC(78,0),
  -- 유닛 상태.
  -- 예시: 'IMPORTED' | 'TOKENIZED'
  status TEXT NOT NULL,
  -- 같은 클래스 내 같은 unit_no 중복 방지.
  UNIQUE (class_id, unit_no)
);
