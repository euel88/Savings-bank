#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
파일 인코딩 문제 해결 유틸리티
기존 Python 파일의 인코딩 문제를 자동으로 수정합니다.
"""

import os
import re
import chardet

def detect_and_fix_encoding(file_path, output_path=None):
    """
    파일의 인코딩을 감지하고 UTF-8로 안전하게 변환합니다.
    
    Args:
        file_path (str): 수정할 파일 경로
        output_path (str): 출력 파일 경로 (None이면 원본 파일 덮어쓰기)
    """
    if output_path is None:
        output_path = file_path
    
    # 1단계: 바이너리로 파일 읽기
    print(f"파일 읽는 중: {file_path}")
    
    with open(file_path, 'rb') as f:
        raw_data = f.read()
    
    # 2단계: 인코딩 자동 감지
    detected = chardet.detect(raw_data)
    detected_encoding = detected.get('encoding', 'utf-8')
    confidence = detected.get('confidence', 0)
    
    print(f"감지된 인코딩: {detected_encoding} (신뢰도: {confidence:.2f})")
    
    # 3단계: 여러 인코딩으로 시도하여 읽기
    encodings_to_try = [detected_encoding, 'utf-8', 'cp949', 'euc-kr', 'latin1']
    
    content = None
    successful_encoding = None
    
    for encoding in encodings_to_try:
        try:
            content = raw_data.decode(encoding)
            successful_encoding = encoding
            print(f"성공한 인코딩: {encoding}")
            break
        except (UnicodeDecodeError, LookupError) as e:
            print(f"인코딩 시도 실패 ({encoding}): {e}")
            continue
    
    if content is None:
        # 최후의 수단: errors='replace'로 읽기
        print("모든 인코딩 시도 실패, 대체 문자로 읽기 시도...")
        content = raw_data.decode('utf-8', errors='replace')
        successful_encoding = 'utf-8 (with replacement)'
    
    # 4단계: 문제 문자 제거 및 수정
    print("문제 문자 정리 중...")
    
    # 대체 문자(�) 제거
    content = content.replace('\ufffd', '')
    
    # 보이지 않는 제어 문자 제거
    content = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', content)
    
    # 불필요한 BOM 제거
    if content.startswith('\ufeff'):
        content = content[1:]
        print("BOM 문자 제거됨")
    
    # 5단계: f-string을 안전한 형태로 변환
    print("f-string을 안전한 형태로 변환 중...")
    content = fix_fstrings_in_content(content)
    
    # 6단계: UTF-8로 저장
    print(f"UTF-8로 저장 중: {output_path}")
    
    with open(output_path, 'w', encoding='utf-8', newline='\n') as f:
        f.write(content)
    
    print("파일 수정 완료!")
    return successful_encoding

def fix_fstrings_in_content(content):
    """
    f-string을 안전한 문자열 결합으로 변환합니다.
    """
    # 간단한 f-string 패턴 찾아서 변환
    # 예: f"Hello {name}" -> "Hello " + str(name)
    
    def replace_fstring(match):
        fstring_content = match.group(1)
        
        # 중괄호 내용과 일반 텍스트 분리
        parts = []
        current_part = ""
        brace_count = 0
        in_expression = False
        
        i = 0
        while i < len(fstring_content):
            char = fstring_content[i]
            
            if char == '{' and not in_expression:
                if current_part:
                    parts.append(('text', current_part))
                    current_part = ""
                in_expression = True
                brace_count = 1
            elif char == '{' and in_expression:
                current_part += char
                brace_count += 1
            elif char == '}' and in_expression:
                brace_count -= 1
                if brace_count == 0:
                    parts.append(('expr', current_part))
                    current_part = ""
                    in_expression = False
                else:
                    current_part += char
            else:
                current_part += char
            
            i += 1
        
        if current_part:
            parts.append(('text', current_part))
        
        # 문자열 결합으로 변환
        if not parts:
            return '""'
        
        result_parts = []
        for part_type, part_content in parts:
            if part_type == 'text':
                # 따옴표 이스케이프
                escaped_content = part_content.replace('"', '\\"')
                result_parts.append(f'"{escaped_content}"')
            else:  # expression
                result_parts.append(f'str({part_content})')
        
        return ' + '.join(result_parts)
    
    # f-string 패턴 매칭 및 변환
    # f"..." 또는 f'...' 패턴 찾기
    fstring_pattern = r'f"([^"]*)"'
    content = re.sub(fstring_pattern, replace_fstring, content)
    
    fstring_pattern = r"f'([^']*)'"
    content = re.sub(fstring_pattern, replace_fstring, content)
    
    return content

def main():
    """
    메인 실행 함수
    """
    input_file = "bank_scraper_headless.py"  # 원본 파일명
    backup_file = "bank_scraper_headless_backup.py"  # 백업 파일명
    
    # 파일 존재 확인
    if not os.path.exists(input_file):
        print(f"오류: {input_file} 파일을 찾을 수 없습니다.")
        print("현재 디렉토리의 파일 목록:")
        for f in os.listdir('.'):
            if f.endswith('.py'):
                print(f"  - {f}")
        return
    
    # 백업 생성
    print(f"백업 파일 생성: {backup_file}")
    with open(input_file, 'rb') as src, open(backup_file, 'wb') as dst:
        dst.write(src.read())
    
    # 인코딩 수정
    try:
        successful_encoding = detect_and_fix_encoding(input_file)
        print(f"\n✅ 성공적으로 수정되었습니다!")
        print(f"   원본 인코딩: {successful_encoding}")
        print(f"   수정된 파일: {input_file}")
        print(f"   백업 파일: {backup_file}")
        
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        print("백업 파일을 확인하세요.")

if __name__ == "__main__":
    # chardet 패키지가 필요합니다
    try:
        import chardet
    except ImportError:
        print("chardet 패키지가 필요합니다.")
        print("다음 명령어로 설치하세요: pip install chardet")
        exit(1)
    
    main()
