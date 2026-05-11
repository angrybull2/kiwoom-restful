from __future__ import annotations

from typing import Optional

from msgspec import Raw, Struct, field


class RealData:
    """
    Real data wrapper class.

    """

    def __init__(
        self,
        values: bytes | Raw,
        type: str,
        name: str,
        item: str,
    ):
        """
        Initialize RealData instance.

        'values' can be used as bytes itself.
            values_str = values.decode('utf-8')
            values_dic = orjson.loads(values)
            values_dic = msgspec.json.decode(values)

        Args:
            values (bytes | Raw): utf-8 encoded bytes or msgspec Raw in opt-in zero-copy mode
            type (str): API type (ex. OB, 0D, ...)
            name (str): API name (ex. 주식체결, 주식호가잔량, ...)
            item (str): stock code
        """
        self.values = values  # utf-8 encoded bytes
        self.type = type  # API type (ex. OB, 0D, ...)
        self.name = name  # API name (ex. 주식체결, 주식호가잔량, ...)
        self.item = item  # stock code

    def __repr__(self):
        return (
            f"RealData(values={self.values}, type={self.type}, name={self.name}, item={self.item})"
        )


# To decode raw json with msgspec
class RealType(Struct):
    class Data(Struct):
        values: Raw
        type: str
        name: str
        item: str

    trnm: str
    data: Optional[list[RealType.Data]] = None


# To decode raw json with msgspec
class TickValuesType(Struct):
    """
    To decode Real.data.values of type 'OB'
    """

    # Real.Data.values
    v20: str = field(name="20")  # 체결시간
    v10: str = field(name="10")  # 현재가
    v15: str = field(name="15")  # +-거래량


# To decode raw json with msgspec
class HogaValuesType(Struct):
    """
    To decode Real.data.values of type '0D'
    """

    # Real.Data.Values
    v21: str = field(name="21")  # 호가시간

    v41: str = field(name="41")  # 매도호가 1
    v61: str = field(name="61")  # 매도잔량 1
    v51: str = field(name="51")  # 매수호가 1
    v71: str = field(name="71")  # 매수잔량 1

    v42: str = field(name="42")  # 매도호가 2
    v62: str = field(name="62")  # 매도잔량 2
    v52: str = field(name="52")  # 매수호가 2
    v72: str = field(name="72")  # 매수잔량 2

    v43: str = field(name="43")  # 매도호가 3
    v63: str = field(name="63")  # 매도잔량 3
    v53: str = field(name="53")  # 매수호가 3
    v73: str = field(name="73")  # 매수잔량 3

    v44: str = field(name="44")  # 매도호가 4
    v64: str = field(name="64")  # 매도잔량 4
    v54: str = field(name="54")  # 매수호가 4
    v74: str = field(name="74")  # 매수잔량 4

    v45: str = field(name="45")  # 매도호가 5
    v65: str = field(name="65")  # 매도잔량 5
    v55: str = field(name="55")  # 매수호가 5
    v75: str = field(name="75")  # 매수잔량 5

    v46: str = field(name="46")  # 매도호가 6
    v66: str = field(name="66")  # 매도잔량 6
    v56: str = field(name="56")  # 매수호가 6
    v76: str = field(name="76")  # 매수잔량 6

    v47: str = field(name="47")  # 매도호가 7
    v67: str = field(name="67")  # 매도잔량 7
    v57: str = field(name="57")  # 매수호가 7
    v77: str = field(name="77")  # 매수잔량 7

    v48: str = field(name="48")  # 매도호가 8
    v68: str = field(name="68")  # 매도잔량 8
    v58: str = field(name="58")  # 매수호가 8
    v78: str = field(name="78")  # 매수잔량 8

    v49: str = field(name="49")  # 매도호가 9
    v69: str = field(name="69")  # 매도잔량 9
    v59: str = field(name="59")  # 매수호가 9
    v79: str = field(name="79")  # 매수잔량 9

    v50: str = field(name="50")  # 매도호가 10
    v70: str = field(name="70")  # 매도잔량 10
    v60: str = field(name="60")  # 매수호가 10
    v80: str = field(name="80")  # 매수잔량 10


# To decode raw json with msgspec
class ViValuesType(Struct):
    """
    To decode Real.data.values of type '1h'
    """

    # Real.Data.Values
    v9001: str = field(name="9001")  # 종목코드
    v302: str = field(name="302")  # 종목명
    v13: str = field(name="13")  # 누적거래량
    v14: str = field(name="14")  # 누적거래대금
    v9068: str = field(name="9068")  # VI발동구분
    v9008: str = field(name="9008")  # KOSPI,KOSDAQ,전체구분
    v9075: str = field(name="9075")  # 장전구분
    v1221: str = field(name="1221")  # VI발동가격
    v1223: str = field(name="1223")  # 매매체결처리시각
    v1224: str = field(name="1224")  # VI해제시각
    v1225: str = field(name="1225")  # VI적용구분
    v1236: str = field(name="1236")  # 기준가격 정적
    v1237: str = field(name="1237")  # 기준가격 동적
    v1238: str = field(name="1238")  # 괴리율 정적
    v1239: str = field(name="1239")  # 괴리율 동적
    v1489: str = field(name="1489")  # VI발동가 등락율
    v1490: str = field(name="1490")  # VI발동횟수
    v9069: str = field(name="9069")  # 발동방향구분
    v1279: str = field(name="1279")  # Extra Item


# To decode raw json with msgspec
class MarketOpenTimeValuesType(Struct):
    """
    To decode Real.data.values of type '0s'
    """

    # Real.Data.Values
    v215: str = field(name="215")  # 장운영구분
    v20: str = field(name="20")  # 체결시간
    v214: str = field(name="214")  # 장시작예상잔여시간


class Types:
    TICK = {
        "20": int,  # 체결시간
        "10": int,  # 현재가
        "15": int,  # +-거래량
    }

    HOGA = {
        "21": int,  # 호가시간
        "41": int,  # 매도호가 1
        "61": int,  # 매도잔량 1
        "51": int,  # 매수호가 1
        "71": int,  # 매수잔량 1
        "42": int,  # 매도호가 2
        "62": int,  # 매도잔량 2
        "52": int,  # 매수호가 2
        "72": int,  # 매수잔량 2
        "43": int,  # 매도호가 3
        "63": int,  # 매도잔량 3
        "53": int,  # 매수호가 3
        "73": int,  # 매수잔량 3
        "44": int,  # 매도호가 4
        "64": int,  # 매도잔량 4
        "54": int,  # 매수호가 4
        "74": int,  # 매수잔량 4
        "45": int,  # 매도호가 5
        "65": int,  # 매도잔량 5
        "55": int,  # 매수호가 5
        "75": int,  # 매수잔량 5
        "46": int,  # 매도호가 6
        "66": int,  # 매도잔량 6
        "56": int,  # 매수호가 6
        "76": int,  # 매수잔량 6
        "47": int,  # 매도호가 7
        "67": int,  # 매도잔량 7
        "57": int,  # 매수호가 7
        "77": int,  # 매수잔량 7
        "48": int,  # 매도호가 8
        "68": int,  # 매도잔량 8
        "58": int,  # 매수호가 8
        "78": int,  # 매수잔량 8
        "49": int,  # 매도호가 9
        "69": int,  # 매도잔량 9
        "59": int,  # 매수호가 9
        "79": int,  # 매수잔량 9
        "50": int,  # 매도호가 10
        "70": int,  # 매도잔량 10
        "60": int,  # 매수호가 10
        "80": int,  # 매수잔량 10
    }

    VI = {
        "9001": str,  # 종목코드
        "302": str,  # 종목명
        "13": str,  # 누적거래량
        "14": str,  # 누적거래대금
        "9068": str,  # VI발동구분
        "9008": str,  # KOSPI,KOSDAQ,전체구분
        "9075": str,  # 장전구분
        "1221": str,  # VI발동가격
        "1223": str,  # 매매체결처리시각
        "1224": str,  # VI해제시각
        "1225": str,  # VI적용구분
        "1236": str,  # 기준가격 정적
        "1237": str,  # 기준가격 동적
        "1238": str,  # 괴리율 정적
        "1239": str,  # 괴리율 동적
        "1489": str,  # VI발동가 등락율
        "1490": str,  # VI발동횟수
        "9069": str,  # 발동방향구분
        "1279": str,  # Extra Item
    }

    MARKET_OPEN_TIME = {
        "215": str,  # 장운영구분
        "20": str,  # 체결시간
        "214": str,  # 장시작예상잔여시간
    }
