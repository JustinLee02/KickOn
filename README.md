# KICK ON 
> 참여형 축구 커뮤니티
> 팬들이 승부 예측에 참여할 수 있는 기능 제공
> 특정 팀 팬들만 접근 가능한 전용 커뮤니티 공간 운영
> AI 기반 선수 이적 가능성 예측 기능을 활용한 데이터 기반 컨텐츠 제공

## 📝 개요
1. 축구 선수의 이적 가능성을 예측하는 기능
   * 선수 프로필 (나이, Market Value, Position, Contract Expired 등) 을 자동으로 스크래핑
   * 데이터 증분 수집 및 전처리 : AWS Lambda 와 Eventbridge를 사용해 주기적 데이터 증분 수집 및 전처리를 통한 학습 데이터 저장
   * 학습 : Sagemaker pipeline을 통한 모델 학습 (XGBoost)
   * OPEN AI API 기반 기사 분석 : GPT 4o mini 모델로 관련 뉴스 기사 요약 및 분석을 통해 보조 확률 산출
   * 예측 & 배포 : XGBoost 예측 확률과 GPT 확률을 조합해 최종 이적 확률 산출 후, SageMaker Endpoint로 배포
   * 백테스트 : S3 archive/ 폴더의 과거 데이터로 전체 파이프라인 성능(정확도) 평가
* * *
2. AI 기반 가상 사용자
   * 다양한 축구 팬 타입 :
     - "열혈 응원단” (팀·선수 경기력 토론)
     - “통계 광” (데이터·그래프 기반 분석)
     - “유머러” (짤·밈·가벼운 농담)
     - “초보 팬” (기초 질문, 가이드 요청)
     타입 별 비율 설정 및 글 생성 (게시물 게시)
   * 스케쥴링
     Eventbridge, lambda 통해 주기적 호출
   * 품질 관리 & 모니터링
     랜덤하게 AI가 생성한 게시글 추출 후 직접 모니터링, 사용자 유치 후 AI 게시글 좋아요 및 댓글을 통해 스크립트 및 주제에 가중치 조정
   * AI 사용자 고지
     AI 게시글이라는 게시글 내 표시 (AI가 생성한 게시글입니다) 및 프로필 뱃지를 통해 사용자에게 고지함
## ⚙️ 기술 스택
   * Python 3.9
  
   * Web Scraping: requests, BeautifulSoup
  
   * AWS 서비스: S3, Lambda, SageMaker (XGBoost, Endpoint, Runtime)
  
   * AI 모델: SageMaker XGBoost, OpenAI GPT-4o-mini
  
   * CI/CD: Docker (Lambda 패키징), EventBridge 스케줄링
  
   * 평가: scikit-learn (accuracy_score)

     
