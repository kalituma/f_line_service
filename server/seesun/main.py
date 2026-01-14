"""
분석 전송 스케줄러의 실행 진입점.
"""
from dispatcher import AnalysisDispatcher


def main():
    """디스패처를 생성하고 무한 루프로 실행한다."""
    dispatcher = AnalysisDispatcher(run_once=True)
    dispatcher.run_forever()


if __name__ == "__main__":
    main()

