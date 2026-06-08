# 🎯 Stream-to-Stream Join: Ad-Click Attribution

## 📌 Giới thiệu

Dự án triển khai hệ thống **Stream-to-Stream Join thời gian thực** cho bài toán **Ad-Click Attribution** — xác định quảng cáo nào dẫn đến click của người dùng, sử dụng **Apache Flink Interval Join**.

Hệ thống nhận hai luồng dữ liệu độc lập chạy song song:

- **Stream A**: `Ad_Impressions` — quảng cáo hiển thị với người dùng `(ImpressionID, UserID, AdID)`
- **Stream B**: `Ad_Clicks` — click của người dùng `(ClickID, ImpressionID, Timestamp)`

Vì clicks xảy ra **SAU** impressions, hệ thống phải buffer impressions trong RAM và chờ clicks đến để join. Đây là bài toán **stateful stream processing** điển hình trong hệ thống phân tán.

## 🧠 Vấn đề chính giải quyết

- **Buffer Memory Management**: Tính toán RAM cần thiết để lưu unjoined impressions trong cửa sổ 1 giờ. Ở traffic bình thường (10,000 impressions/phút), hệ thống cần khoảng **60 MB RAM**.
- **Match Rate**: Đo phần trăm clicks được link thành công với impression trong cửa sổ **10 giây**. Công thức: `matched / total_clicks × 100%`.
- **Stream Join Algorithm**: Join hai luồng theo `ImpressionID` sử dụng **Flink Interval Join** — biến thể của hash join trong lý thuyết Özsu & Valduriez (Ch.4).
- **Failure Cases**: Phân tích và mô phỏng các lỗi thực tế: late click, orphan click, clock skew, buffer overflow.

## 🔄 Kiến trúc hệ thống

```
Stream A: Ad_Impressions                    Stream B: Ad_Clicks
(ImpressionID, UserID, AdID)               (ClickID, ImpressionID, Timestamp)
        │                                              │
        ▼                                              ▼
[assignTimestamps + Watermark]             [assignTimestamps + Watermark]
        │                                              │
        ▼                                              ▼
[keyBy(ImpressionID)]                      [keyBy(ImpressionID)]
        │                                              │
        └──────────── Interval Join ───────────────────┘
                    between(0ms, 10s)
                    (click phải đến SAU impression trong vòng 10 giây)
                           │
              ┌────────────┴────────────┐
              ▼                         ▼
         MATCH ✓                    MISS ✗
   → attributed_clicks          → late click (>10s)
                                → orphan click (không có impression)
                           │
                           ▼
              [Sink: Print to Stdout]
              [parallelism = 2 subtasks]
```

## 🛠️ Công nghệ sử dụng

- **Ngôn ngữ**: Java 25
- **Stream Processing**: Apache Flink 1.17.2 (Interval Join, Event Time, Watermark)
- **Build tool**: Apache Maven 3.8+
- **Thư viện**: `flink-streaming-java`, `flink-clients`, `flink-connector-kafka`, `flink-statebackend-rocksdb`
- **State Backend**: HashMap (in-memory) — phù hợp với demo local
- **Không cần**: cài Kafka, Flink cluster hay database riêng

## ⚙️ Cài đặt và Chạy dự án

### Yêu cầu hệ thống

- Java 25
- Apache Maven 3.8+
- Windows PowerShell (để chạy `run.ps1`)

### Hướng dẫn cài đặt

1. Clone repository:
```
git clone https://github.com/duongvanhay050405/stream-join-ad-attribution.git
cd stream-join-ad-attribution
```

2. Build project:
```
mvn clean package -DskipTests
```

3. Chạy chương trình:
```
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\run.ps1
```

> ⚠️ Lưu ý: Flink 1.17 chưa hỗ trợ chính thức Java 25. File `run.ps1` đã thêm sẵn các JVM flag `--add-opens` cần thiết để chạy ổn định.

## 🚀 Hướng dẫn sử dụng

Chạy `.\run.ps1` — chương trình tự động build và chạy Flink job. Kết quả in thẳng ra terminal:

- `1>` và `2>` là output từ 2 subtasks (vì `parallelism = 2`)
- Các dòng `MATCH` là clicks được join thành công trong cửa sổ 10 giây
- `click_late` và `click_orphan` **không xuất hiện trong output** — đây chính là bằng chứng failure cases hoạt động đúng

## 📊 Kết quả mong đợi

```
2> MATCH: imp3 | user=user3 | ad=ad3 | click=click3 | delay=1300ms
1> MATCH: imp1 | user=user1 | ad=ad1 | click=click1 | delay=1100ms
2> MATCH: imp5 | user=user5 | ad=ad5 | click=click5 | delay=1500ms
1> MATCH: imp2 | user=user2 | ad=ad2 | click=click2 | delay=1200ms
1> MATCH: imp4 | user=user4 | ad=ad4 | click=click4 | delay=1400ms

--- FAILURE CASES (không xuất hiện trong output) ---
click_late   → MISS: đến sau 15,000ms > cửa sổ 10,000ms
click_orphan → MISS: imp_fake_999 không tồn tại trong buffer

Match Rate = 5 matched / 7 total clicks = 71.4%
```

## 📐 Tính toán Buffer RAM

Impression buffer giữ dữ liệu trong RAM để chờ click đến join. Kích thước mỗi record khoảng 100 bytes (3 fields + Python dict overhead).

**Công thức tính:**
```
RAM = impressions_per_min × window_min × bytes_per_record
    = 10,000 × 60 × 100 bytes
    = 60,000,000 bytes
    ≈ 60 MB
```

**Bảng phân tích theo mức traffic:**

| Scenario | Rate/phút | Window | Records giữ | RAM cần |
|----------|-----------|--------|-------------|---------|
| Low | 1,000 | 1 giờ | 60,000 | ~6 MB |
| Normal | 10,000 | 1 giờ | 600,000 | ~60 MB |
| Peak | 100,000 | 1 giờ | 6,000,000 | ~600 MB |
| Join-only | 10,000 | 10 giây | 1,667 | ~167 KB |

> **Nhận xét**: Join window 10s chỉ cần ~167 KB. Buffer window 1h cần ~60 MB. Trade-off: window dài hơn → recall cao hơn nhưng tốn RAM hơn.

## 🔬 Các lỗi được mô phỏng (Failure Cases)

| STT | Tên lỗi | Mô tả | Kết quả quan sát |
|-----|---------|-------|-----------------|
| 1 | Late click (> 10 giây) | `click_late` đến sau 15,000ms, vượt cửa sổ 10s | Không xuất hiện trong output |
| 2 | Orphan click | `click_orphan` dùng `imp_fake_999` không tồn tại | Không xuất hiện trong output |

**Giải thích lý thuyết (Özsu & Valduriez):**
- **Late click** → Liên quan đến Temporal Query Processing (Ch.8): click nằm ngoài interval `[impression_ts, impression_ts + 10s]`
- **Orphan click** → Liên quan đến Semi-join reduction (Ch.4): không có bản ghi nào trong impression side để join
- **Giải pháp** → Dùng Side Output để capture late events, hoặc tăng `forBoundedOutOfOrderness` watermark

## 📁 Cấu trúc project

```
stream-join-ad-attribution/
├── src/
│   └── main/
│       └── java/
│           └── com/join/
│               ├── StreamJoinJob.java       # Main Flink job: interval join + failure cases
│               ├── AdImpressionEvent.java   # Data model: impression event (Serializable)
│               └── AdClickEvent.java        # Data model: click event (Serializable)
├── pom.xml                                  # Maven dependencies (Flink 1.17.2, Java 25)
├── run.ps1                                  # Script build + run (Windows PowerShell)
└── README.md
```

## 🔑 Chi tiết kỹ thuật quan trọng

**Tại sao dùng Interval Join thay vì Window Join?**
- Window Join yêu cầu cả hai stream phải có dữ liệu trong cùng một window → không phù hợp vì impression và click không đồng bộ thời gian
- Interval Join cho phép join theo khoảng thời gian tương đối `[impression_ts + 0ms, impression_ts + 10s]` → đúng với bài toán attribution

**Tại sao dùng Event Time thay vì Processing Time?**
- Processing Time dùng đồng hồ của máy → bị ảnh hưởng bởi clock skew giữa các node
- Event Time dùng timestamp từ dữ liệu gốc → reproducible, chính xác hơn

**Watermark strategy:**
```java
WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
```
Cho phép late events đến muộn tối đa 2 giây trước khi bị drop.

## 🎬 Video demo

Xem video minh họa đầy đủ hoạt động bình thường và các failure cases tại đây:
[Link Google Drive / YouTube sẽ cập nhật sau khi quay]

## 👥 Tác giả

- **Họ tên**: Dương Văn Hay
- **Mã số sinh viên**: N23DCCN087
- **Môn học**: Distributed Databases

## 📜 Giấy phép

MIT License
