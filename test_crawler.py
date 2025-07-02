#!/usr/bin/env python3
"""
간단한 크롤링 테스트 스크립트
실행: python scripts/test_crawler.py
"""

import asyncio
import aiohttp
from datetime import datetime
import sys
import os

# 프로젝트 루트를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

async def test_basic_crawling():
    """기본 크롤링 테스트"""
    print(f"[{datetime.now()}] 크롤링 테스트 시작...")
    
    try:
        async with aiohttp.ClientSession() as session:
            # 간단한 HTTP 요청 테스트
            async with session.get('https://httpbin.org/json') as response:
                if response.status == 200:
                    data = await response.json()
                    print(f"✅ HTTP 요청 성공: {response.status}")
                    print(f"📄 응답 데이터: {data.get('slideshow', {}).get('title', 'N/A')}")
                else:
                    print(f"❌ HTTP 요청 실패: {response.status}")
                    
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
    
    print(f"[{datetime.now()}] 크롤링 테스트 완료!")

if __name__ == "__main__":
    asyncio.run(test_basic_crawling())