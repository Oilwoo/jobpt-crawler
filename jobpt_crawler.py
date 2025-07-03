#!/usr/bin/env python3
"""
채용공고 크롤링 및 Pinecone DB 저장 스크립트 (동적 스크롤 기반)
실행: python jobpt_crawler.py
"""

import os
import json
import asyncio
import time
from uuid import uuid4
from datetime import datetime
import logging
from dotenv import load_dotenv
from typing import Optional, Dict
from bs4 import BeautifulSoup

# Selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

# LangChain & Pinecone
from langchain.docstore.document import Document
from langchain_pinecone import PineconeVectorStore
from langchain_upstage import UpstageEmbeddings
from pinecone import Pinecone, ServerlessSpec

# 환경변수 로드
load_dotenv('.env')

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/crawler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 상수 설정
BASE_URL = "https://www.wanted.co.kr"
LIST_URL = BASE_URL + "/wdlist?country=kr&job_sort=job.popularity_order&years=-1&locations=all"

def clean(text: str) -> str:
    """텍스트 정리 함수 (타입 체크 추가)"""
    if not isinstance(text, str):
        return ""
    return (
        text.replace("•", "")
            .replace("■", "")
            .replace("\n", " ")
            .replace("\\n", " ")
            .replace("  ", " ")
            .replace("\u200b", "")  # 제로폭 공백
            .replace("\xa0", " ")   # 비파괴 공백
            .strip()
    )

def extract_block(div) -> str:
    """div 블록에서 텍스트 추출 (데이터팀 방식)"""
    try:
        # 데이터팀이 사용한 방식 적용
        span = div.select_one("span.wds-h4ga6o span")
        if span:
            return clean(span.get_text(separator="\n"))
        else:
            # 대안: 모든 텍스트 추출 후 정리
            text = div.get_text(separator=" ", strip=True)
            return clean(text)
    except Exception:
        return ""

def setup_chrome_driver():
    """Chrome 드라이버 설정 (우분투 환경 최적화)"""
    try:
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")  # 최신 헤드리스 모드
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        
        # 데이터팀과 동일한 User-Agent 사용
        chrome_options.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        )
        
        # 우분투에서 Chrome 안정성을 위한 추가 옵션
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--disable-images")  # 이미지 로딩 비활성화로 속도 개선
        
        driver = webdriver.Chrome(options=chrome_options)
        logger.info("Chrome 드라이버 설정 완료")
        return driver
    except Exception as e:
        logger.error(f"Chrome 드라이버 설정 실패: {e}")
        return None

def parse_detail(href: str, driver, retry: int = 2) -> Optional[Dict]:
    """채용공고 상세 파싱 함수 (데이터팀 방식 그대로)"""
    url = BASE_URL + href
    err = None
    
    for attempt in range(retry):
        try:
            logger.debug(f"상세 파싱 시도 {attempt + 1}/{retry}: {href}")
            driver.get(url)
            
            # 페이지 로딩 대기
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "h1, h2"))
            )
            
            # '상세 정보 더 보기' 버튼 클릭 시도
            try:
                btn = driver.find_element(
                    By.XPATH, "//button//span[contains(text(),'상세 정보 더 보기')]"
                )
                driver.execute_script("arguments[0].click()", btn)
                time.sleep(0.5)
            except Exception:
                pass  # 버튼이 없으면 무시
            
            soup = BeautifulSoup(driver.page_source, "html.parser")
            
            # 제목과 회사명 추출
            title_el = soup.select_one("header h1, header h2")
            company_el = soup.select_one("header a[data-company-name]")
            title = clean(title_el.text) if title_el else ""
            company = clean(company_el.text) if company_el else ""
            
            # 마감일 추출
            due_article = soup.find("article", class_="JobDueTime_JobDueTime__yvhtg")
            due_date = ""
            if due_article:
                h2 = due_article.find("h2")
                if h2 and "마감일" in h2.get_text():
                    span = due_article.find("span")
                    if span:
                        due_date = clean(span.get_text())
            
            # 근무지역 추출
            work_article = soup.find("article", class_="JobWorkPlace_JobWorkPlace__xPlGe")
            work_location = ""
            if work_article:
                span = work_article.find("span")
                if span:
                    work_location = clean(span.get_text())
            
            # 상세 정보 추출
            detail = {
                "포지션상세": "",
                "주요업무": "",
                "자격요건": "",
                "우대사항": "",
                "채용 전형": "",
                "혜택 및 복지": "",
            }
            
            # 포지션 상세 (별도 div)
            intro_div = soup.select_one("div.JobDescription_JobDescription__paragraph__wrapper__WPrKC")
            if intro_div:
                detail["포지션상세"] = extract_block(intro_div)
            
            # 나머지 상세 정보들
            for div in soup.select("div.JobDescription_JobDescription__paragraph__87w8I"):
                h3 = div.find("h3")
                if not h3:
                    continue
                key = clean(h3.text)
                value = extract_block(div)
                if key in detail:
                    detail[key] = value
            
            # 성공적으로 파싱된 경우
            result = {
                "url": url,
                "title": title,
                "company": company,
                "마감일": due_date,
                "근무지역": work_location,
                **detail
            }
            
            # 최소한의 정보가 있는지 확인
            if title or company:
                return result
            else:
                logger.warning(f"빈 데이터 감지: {href}")
                continue
                
        except Exception as e:
            err = e
            logger.debug(f"파싱 시도 {attempt + 1} 실패: {e}")
            time.sleep(0.5)
    
    logger.warning(f"상세 파싱 최종 실패 {href} → {err}")
    return None

class JobCrawlerPinecone:
    def __init__(self):
        """크롤러 및 Pinecone 초기화"""
        # API 키 확인
        self.upstage_api_key = os.getenv('UPSTAGE_API_KEY')
        self.pinecone_api_key = os.getenv('PINECONE_API_KEY')
        
        if not self.upstage_api_key or not self.pinecone_api_key:
            raise ValueError("UPSTAGE_API_KEY와 PINECONE_API_KEY가 필요합니다!")
        
        # Pinecone 설정
        self.index_name = os.getenv('PINECONE_INDEX_NAME', 'job-postings')
        self.pc = Pinecone(api_key=self.pinecone_api_key)
        
        # 임베딩 모델
        self.embedding_model = UpstageEmbeddings(
            model="embedding-query",
            api_key=self.upstage_api_key
        )
        
        # 크롤링 설정 (데이터팀 방식 적용)
        self.max_links = int(os.getenv('MAX_LINKS', 500))
        self.scroll_pause = float(os.getenv('SCROLL_PAUSE', 1.2))
        self.crawl_delay = int(os.getenv('CRAWL_DELAY', 2))
        
        # Selenium 드라이버
        self.driver = None
        
        logger.info("JobCrawlerPinecone 초기화 완료")

    def setup_pinecone_index(self):
        """Pinecone 인덱스 생성 또는 연결"""
        try:
            # 기존 인덱스 확인
            if self.index_name not in self.pc.list_indexes().names():
                logger.info(f"새 인덱스 생성: {self.index_name}")
                self.pc.create_index(
                    name=self.index_name,
                    dimension=4096,  # Upstage embedding 차원
                    metric="cosine",
                    spec=ServerlessSpec(cloud="aws", region="us-east-1")
                )
            else:
                logger.info(f"기존 인덱스 사용: {self.index_name}")
            
            # 인덱스 연결
            self.index = self.pc.Index(self.index_name)
            
            # Vectorstore 생성
            self.vectorstore = PineconeVectorStore(
                index=self.index, 
                embedding=self.embedding_model
            )
            
            logger.info("Pinecone 인덱스 설정 완료")
            return True
            
        except Exception as e:
            logger.error(f"Pinecone 설정 오류: {e}")
            return False

    def setup_driver(self):
        """Selenium 드라이버 초기화"""
        self.driver = setup_chrome_driver()
        if not self.driver:
            raise ValueError("Chrome 드라이버 설정 실패")
        return True

    def collect_job_links(self):
        """동적 스크롤을 통한 채용공고 링크 수집 (데이터팀 방식)"""
        logger.info("동적 스크롤 기반 링크 수집 시작...")
        
        try:
            # 원티드 목록 페이지 접속
            self.driver.get(LIST_URL)
            logger.info(f"접속 URL: {LIST_URL}")
            
            # 첫 번째 카드가 로드될 때까지 대기
            WebDriverWait(self.driver, 30).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, 'div[data-cy="job-card"] a[href^="/wd/"]')
                )
            )
            logger.info("초기 페이지 로딩 완료")
            
            hrefs = set()
            void_cnt = 0  # 새 링크가 없는 횟수
            last_height = 0
            
            logger.info(f"목표 링크 수: {self.max_links}개")
            
            while len(hrefs) < self.max_links and void_cnt < 3:
                # 현재 페이지에서 링크 추출
                soup = BeautifulSoup(self.driver.page_source, "html.parser")
                
                before_count = len(hrefs)
                
                # 채용공고 카드에서 링크 추출
                for a in soup.select('div[data-cy="job-card"] a[href^="/wd/"]'):
                    href = a.get("href")
                    if href:
                        hrefs.add(href)
                
                added_count = len(hrefs) - before_count
                
                if added_count > 0:
                    logger.info(f"새 링크 {added_count}개 발견, 총 {len(hrefs)}개")
                    void_cnt = 0
                else:
                    void_cnt += 1
                    logger.info(f"새 링크 없음 ({void_cnt}/3)")
                
                # 목표 달성 확인
                if len(hrefs) >= self.max_links:
                    break
                
                # 페이지 끝까지 스크롤
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(self.scroll_pause)
                
                # 페이지 높이 변화 확인
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    logger.info("더 이상 스크롤할 내용이 없습니다")
                    break
                last_height = new_height
            
            final_links = list(hrefs)[:self.max_links]
            logger.info(f"링크 수집 완료: {len(final_links)}개")
            
            return final_links
            
        except Exception as e:
            logger.error(f"링크 수집 오류: {e}")
            return []

    def crawl_job_details(self, job_links):
        """채용공고 상세 정보 크롤링"""
        logger.info(f"{len(job_links)}개 채용공고 상세 정보 크롤링 시작...")
        jobs = []
        
        for i, href in enumerate(job_links):
            logger.info(f"[{i+1}/{len(job_links)}] 상세 정보 수집: {href}")
            
            # 상세 정보 파싱
            job_detail = parse_detail(href, self.driver)
            
            if job_detail:
                # 메타데이터 추가
                job_detail.update({
                    "source": "wanted",
                    "crawled_at": datetime.now().isoformat(),
                    "link_index": i + 1
                })
                jobs.append(job_detail)
                logger.info(f"✅ 수집 성공: {job_detail.get('company', 'N/A')} - {job_detail.get('title', 'N/A')}")
            else:
                logger.warning(f"❌ 수집 실패: {href}")
            
            # 요청 간격 조절 (원티드 서버 부하 방지)
            time.sleep(self.crawl_delay)
        
        logger.info(f"상세 정보 크롤링 완료: {len(jobs)}개 성공")
        return jobs

    def create_documents(self, jobs):
        """채용공고 데이터를 Document 객체로 변환 (신입경력, source_file, original_url 필드 반영)"""
        documents = []
        for job in jobs:
            desc = job
            merged_text = " | ".join([
                f"url: {job.get('url', '')}",
                f"title: {job.get('title', '')}",
                f"company: {job.get('company', '')}",
                f"마감일: {job.get('마감일','')}",
                f"근무지역: {clean(job.get('근무지역',''))}",
                f"신입경력: {clean(job.get('신입경력', ''))}",
                f"포지션상세: {clean(desc.get('포지션상세', ''))}",
                f"주요업무: {clean(desc.get('주요업무', ''))}",
                f"자격요건: {clean(desc.get('자격요건', ''))}",
                f"우대사항: {clean(desc.get('우대사항', ''))}",
                f"채용 전형: {clean(desc.get('채용 전형', ''))}",
                f"혜택 및 복지: {clean(desc.get('혜택 및 복지', ''))}",
            ])
            metadata = {
                "url": job.get("url", ""),
                "title": job.get("title", ""),
                "company": job.get("company", ""),
                "마감일": job.get("마감일",""),
                "근무지역": job.get("근무지역",""),
                "신입경력": job.get("신입경력",""),
                "source_file": job.get("metadata", {}).get("source_file", ""),
                "original_url": job.get("metadata", {}).get("original_url", ""),
            }
            documents.append(Document(page_content=merged_text, metadata=metadata))
        logger.info(f"{len(documents)}개 Document 객체 생성")
        return documents

    async def save_to_pinecone(self, documents):
        """문서들을 Pinecone에 저장"""
        if not documents:
            logger.warning("저장할 문서가 없습니다")
            return 0
        
        logger.info(f"{len(documents)}개 문서를 Pinecone에 저장 시작...")
        
        try:
            # 배치 단위로 저장
            batch_size = 10
            total_saved = 0
            
            for i in range(0, len(documents), batch_size):
                batch = documents[i:i + batch_size]
                ids = [str(uuid4()) for _ in batch]
                
                try:
                    self.vectorstore.add_documents(documents=batch, ids=ids)
                    total_saved += len(batch)
                    logger.info(f"배치 {i//batch_size + 1}: {len(batch)}개 문서 저장 완료")
                    
                except Exception as e:
                    logger.error(f"배치 {i//batch_size + 1} 저장 오류: {e}")
                    continue
                
                # 배치 간 대기
                await asyncio.sleep(0.5)
            
            logger.info(f"총 {total_saved}개 문서 Pinecone 저장 완료")
            return total_saved
            
        except Exception as e:
            logger.error(f"Pinecone 저장 중 오류: {e}")
            return 0

    def save_raw_data(self, jobs, filename=None):
        """원본 데이터를 JSON 파일로 백업"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"data/crawled_jobs_{timestamp}.json"
        
        try:
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(jobs, f, ensure_ascii=False, indent=2)
            
            logger.info(f"원본 데이터 저장: {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"원본 데이터 저장 오류: {e}")
            return None

    def cleanup(self):
        """리소스 정리"""
        if self.driver:
            self.driver.quit()
            logger.info("Chrome 드라이버 종료")

async def main():
    """메인 실행 함수"""
    logger.info("=" * 60)
    logger.info("원티드 채용공고 동적 스크롤 크롤링 및 Pinecone 저장 시작")
    logger.info("=" * 60)
    
    crawler = None
    try:
        # 크롤러 초기화
        crawler = JobCrawlerPinecone()
        
        # Pinecone 설정
        if not crawler.setup_pinecone_index():
            logger.error("Pinecone 설정 실패")
            return
        
        # Selenium 드라이버 설정
        if not crawler.setup_driver():
            logger.error("Selenium 드라이버 설정 실패")
            return
        
        # 1. 동적 스크롤로 채용공고 링크 수집
        job_links = crawler.collect_job_links()
        if not job_links:
            logger.warning("수집된 채용공고 링크가 없습니다")
            return
        
        # 2. 상세 정보 크롤링
        jobs = crawler.crawl_job_details(job_links)
        if not jobs:
            logger.warning("크롤링된 채용공고가 없습니다")
            return
        
        # 3. 원본 데이터 백업
        backup_file = crawler.save_raw_data(jobs)
        
        # 4. Document 객체로 변환
        documents = crawler.create_documents(jobs)
        
        # 5. Pinecone에 저장
        saved_count = await crawler.save_to_pinecone(documents)
        
        # 6. 결과 요약
        summary = {
            "total_links": len(job_links),
            "total_crawled": len(jobs),
            "total_saved": saved_count,
            "success_rate": f"{len(jobs)/len(job_links)*100:.1f}%" if job_links else "0%",
            "backup_file": backup_file,
            "timestamp": datetime.now().isoformat()
        }
        
        # 통계 로그 저장
        os.makedirs('logs', exist_ok=True)
        with open('logs/crawl_summary.json', 'a') as f:
            f.write(json.dumps(summary, ensure_ascii=False) + '\n')
        
        logger.info("=" * 60)
        logger.info("크롤링 및 저장 완료!")
        logger.info(f"링크 수집: {summary['total_links']}개")
        logger.info(f"상세 수집: {summary['total_crawled']}개 (성공률: {summary['success_rate']})")
        logger.info(f"Pinecone 저장: {summary['total_saved']}개")
        logger.info(f"백업 파일: {summary['backup_file']}")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"메인 프로세스 오류: {e}")
        raise
    finally:
        # 리소스 정리
        if crawler:
            crawler.cleanup()

if __name__ == "__main__":
    asyncio.run(main())