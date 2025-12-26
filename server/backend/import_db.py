"""
배치 import를 위한 유틸리티 모듈

여러 JSON 파일을 한 번에 처리하거나, 
프로그래매틱하게 import를 수행할 때 사용합니다.
"""

import logging
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional

# 프로젝트 루트 경로 추가
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from server.backend.service.video_service import WildfireVideoService

logger = logging.getLogger(__name__)


class BatchVideoImporter:
    """여러 JSON 파일을 배치 처리하는 클래스"""
    
    def __init__(self):
        self.service = WildfireVideoService()
        self.results = []
        logger.info("BatchVideoImporter initialized")
    
    def import_single_file(self, config_file_path: str) -> Dict[str, Any]:
        """
        단일 파일 import
        
        Args:
            config_file_path: 설정 파일 경로
            
        Returns:
            import 결과
        """
        logger.info(f"Importing from: {config_file_path}")
        result = self.service.import_from_config_file(config_file_path)
        result['file'] = config_file_path
        self.results.append(result)
        return result
    
    def import_multiple_files(self, file_paths: List[str]) -> List[Dict[str, Any]]:
        """
        여러 파일 배치 import
        
        Args:
            file_paths: 설정 파일 경로 리스트
            
        Returns:
            각 파일별 import 결과 리스트
        """
        logger.info(f"Batch importing {len(file_paths)} files")
        
        for file_path in file_paths:
            if not Path(file_path).exists():
                logger.warning(f"File not found: {file_path}")
                self.results.append({
                    'file': file_path,
                    'success': False,
                    'error': 'File not found'
                })
                continue
            
            self.import_single_file(file_path)
        
        return self.results
    
    def import_from_directory(self, directory_path: str, pattern: str = "*.json") -> List[Dict[str, Any]]:
        """
        디렉토리의 모든 JSON 파일 import
        
        Args:
            directory_path: 디렉토리 경로
            pattern: 파일 패턴 (기본값: "*.json")
            
        Returns:
            각 파일별 import 결과 리스트
        """
        dir_path = Path(directory_path)
        
        if not dir_path.exists():
            logger.error(f"Directory not found: {directory_path}")
            return []
        
        json_files = list(dir_path.glob(pattern))
        logger.info(f"Found {len(json_files)} JSON files in {directory_path}")
        
        return self.import_multiple_files([str(f) for f in json_files])
    
    def get_summary(self) -> Dict[str, Any]:
        """
        배치 import 결과 요약
        
        Returns:
            전체 통계
        """
        total_files = len(self.results)
        successful_files = sum(1 for r in self.results if r.get('success', False))
        total_videos = sum(r.get('total', 0) for r in self.results if r.get('success'))
        total_imported = sum(r.get('imported', 0) for r in self.results if r.get('success'))
        total_failed = sum(r.get('failed', 0) for r in self.results if r.get('success'))
        
        return {
            'total_files': total_files,
            'successful_files': successful_files,
            'failed_files': total_files - successful_files,
            'total_videos': total_videos,
            'total_imported': total_imported,
            'total_video_failures': total_failed,
            'results': self.results
        }
    
    def print_summary(self) -> None:
        """배치 결과 출력"""
        summary = self.get_summary()
        
        print("\n" + "=" * 80)
        print("배치 Import 결과 요약".center(80))
        print("=" * 80)
        
        print(f"\n📊 파일 통계:")
        print(f"  총 파일: {summary['total_files']}개")
        print(f"  ✅ 성공: {summary['successful_files']}개")
        print(f"  ❌ 실패: {summary['failed_files']}개")
        
        print(f"\n📹 비디오 통계:")
        print(f"  총 비디오: {summary['total_videos']}개")
        print(f"  ✔️ Import 성공: {summary['total_imported']}개")
        print(f"  ❌ Import 실패: {summary['total_video_failures']}개")
        
        # 파일별 상세 결과
        print(f"\n📋 파일별 상세 결과:")
        for result in summary['results']:
            status = "✅" if result.get('success') else "❌"
            file_name = Path(result['file']).name
            
            if result.get('success'):
                print(
                    f"  {status} {file_name}: "
                    f"{result['imported']}/{result['total']} "
                    f"({result['failed']} 실패)"
                )
            else:
                print(f"  {status} {file_name}: 실패 - {result.get('error', 'Unknown error')}")
        
        print("\n" + "=" * 80 + "\n")


def import_single(config_file_path: str) -> Dict[str, Any]:
    """
    단일 파일 import (편의 함수)
    
    Args:
        config_file_path: 설정 파일 경로
        
    Returns:
        import 결과
    """
    service = WildfireVideoService()
    return service.import_from_config_file(config_file_path)


def import_batch(file_paths: List[str]) -> List[Dict[str, Any]]:
    """
    배치 import (편의 함수)
    
    Args:
        file_paths: 설정 파일 경로 리스트
        
    Returns:
        각 파일별 import 결과 리스트
    """
    importer = BatchVideoImporter()
    return importer.import_multiple_files(file_paths)


def import_directory(directory_path: str) -> List[Dict[str, Any]]:
    """
    디렉토리 import (편의 함수)
    
    Args:
        directory_path: 디렉토리 경로
        
    Returns:
        각 파일별 import 결과 리스트
    """
    importer = BatchVideoImporter()
    importer.import_from_directory(directory_path)
    return importer.results


if __name__ == '__main__':
    # 테스트 예제
    logging.basicConfig(level=logging.INFO)
    
    # 단일 파일 import
    result = import_single("config/video_config.1.json")
    print(f"Result: {result}")

