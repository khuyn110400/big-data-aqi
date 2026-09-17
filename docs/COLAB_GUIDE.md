# Hướng dẫn chạy backfill + Pha 1→2→3 trên Google Colab

> Dành cho NGƯỜI A, sau khi đã viết/test xong ở Mac (`docker/`, `collector/`, `cities.json` —
> xem `WORKPLAN.md` §0). File này chỉ nói về việc **chạy** trên Colab, không nói lại phần code.

---

## 0. Trước khi bắt đầu — checklist

- [ ] `collector/src/backfill_history.py` đã test `--dry-run` và `--limit 2` ở Mac, ra đúng schema C1
- [ ] `collector/config/cities.json` đã có đủ ~200 trạm
- [ ] Đã có API key OpenWeather (key mới tạo cần chờ 10 phút – 2 tiếng mới active)
- [ ] Đã có tài khoản Google (để dùng Colab + Drive)
- [ ] Các thay đổi đã **push lên GitHub** — Colab clone code từ GitHub, không đọc trực tiếp máy Mac

---

## Bước 1 — Push code lên GitHub

```bash
git add collector/ docs/colab_collect.ipynb docs/COLAB_GUIDE.md
git commit -m "feat(a): backfill_history.py, mo rong cities.json 200 tram, colab notebook"
git push origin claude/exciting-cannon-artc3j
```

> Nếu làm theo quy ước PR trong WORKPLAN (`không push thẳng main`), mở PR vào `main` sau khi đẩy
> nhánh — Colab vẫn clone được trực tiếp từ nhánh `claude/exciting-cannon-artc3j`, không cần chờ
> merge.

**Repo phải ở chế độ Colab clone được không cần SSH key** — kiểm tra bằng cách mở thử
`https://github.com/khuyn110400/big-data-aqi` trên trình duyệt ẩn danh (không đăng nhập). Nếu
repo private, Colab cần bạn đăng nhập GitHub trong notebook (`!git clone` sẽ hỏi username/token)
hoặc dùng Personal Access Token trong URL clone.

---

## Bước 2 — Mở notebook trên Colab

1. Vào [colab.research.google.com](https://colab.research.google.com)
2. **File → Upload notebook → GitHub** (tab thứ 3), dán URL repo hoặc tìm
   `khuyn110400/big-data-aqi`, chọn nhánh `claude/exciting-cannon-artc3j`, chọn file
   `docs/colab_collect.ipynb`
   - Cách khác: mở trực tiếp bằng URL dạng
     `https://colab.research.google.com/github/khuyn110400/big-data-aqi/blob/claude/exciting-cannon-artc3j/docs/colab_collect.ipynb`
3. Đổi tên bản trên Colab nếu muốn (không ảnh hưởng file gốc trong repo — Colab làm việc trên
   bản sao cho tới khi bạn tự "Save a copy to GitHub").

---

## Bước 3 — Thêm API key vào Colab Secrets (làm 1 lần)

1. Bấm icon 🔑 **Secrets** ở thanh bên trái.
2. **Add new secret**: tên `OWM_API_KEY`, giá trị = key OpenWeather của bạn.
3. Bật toggle **Notebook access** cho secret này (mặc định tắt — quên bước này thì Cell 3 báo lỗi
   "Secret not found" hoặc trả về rỗng).

> Secret chỉ lưu trong tài khoản Google của bạn, không nằm trong notebook, không bị commit khi
> "Save a copy to GitHub" — an toàn hơn hard-code.

---

## Bước 4 — Chạy Cell 1, 1b: lấy code + cài đặt

Chạy 2 cell đầu (nút ▶ bên trái từng cell, hoặc `Shift+Enter`).

- Cell 1: clone repo, cài `collector/requirements.txt` + `spark/requirements.txt` — mất ~1-2 phút.
- Cell 1b: cài OpenJDK 17 (Spark 3.5 cần Java 17, **không phải Java 21** — lỗi reflection nếu
  dùng bản mặc định của Colab) — mất ~30 giây. Cuối cell in ra `openjdk version "17...` là đúng.

**Runtime → Change runtime type**: không cần GPU/TPU cho job này, để mặc định CPU là đủ.

---

## Bước 5 — Chạy Cell 2: mount Google Drive

Chạy cell, một popup xin quyền truy cập Drive sẽ hiện ra → **Allow**. Sau khi mount xong, thư mục
đích được tạo tại:

```
/content/drive/MyDrive/big-data-aqi/air-quality/
```

Đây là nơi **duy nhất** dữ liệu tồn tại lâu dài — `/content` (đĩa local của phiên Colab) sẽ mất
sạch khi phiên kết thúc.

---

## Bước 6 — Chạy Cell 3: đọc API key từ Secrets

Chạy cell, một popup **"This notebook is requesting access to secret OWM_API_KEY"** hiện ra lần
đầu → **Grant access**. Cell in ra độ dài key để xác nhận đọc được (không in giá trị thật).

- Nếu thấy lỗi `Secret not found`: quay lại Bước 3, kiểm tra tên secret đúng `OWM_API_KEY` và đã
  bật Notebook access.

---

## Bước 7 — Chạy Cell 3b: dry-run xác nhận trước khi tốn quota

```
200 trạm x 23 cửa sổ (~90 ngày) = 4.600 call dự kiến (~76.7 phút ở 1 call/giây)
```

Đây là con số đã kiểm chứng ở Mac. Nếu số trạm/cửa sổ in ra khác hẳn (ví dụ 8 trạm thay vì 200),
dừng lại — `cities.json` trên GitHub chưa đúng bản đã mở rộng, đừng chạy Cell 4.

---

## Bước 8 — Chạy Cell 4: backfill thật (~1,3 giờ)

```bash
!python collector/src/backfill_history.py \
    --cities collector/config/cities.json \
    --from 2021-01-01 --to 2026-09-01 \
    --out {DATA}/raw --resume
```

- Chạy lâu (~1,3 giờ) — **giữ tab trình duyệt hoạt động** (đừng để máy sleep), Colab free ngắt sau
  ~90 phút không tương tác.
- Log in ra theo từng trạm hoàn tất: `VN_HCM_01 xong (23 call dùng, ... bản ghi, ... dlq)`.
- **Nếu bị ngắt kết nối giữa chừng**: `Runtime → Reconnect`, chạy lại **từ Cell 1** (phải cài lại
  deps vì `/content` mất sạch), rồi chạy tiếp đến Cell 4 — cờ `--resume` đọc
  `{DATA}/raw/_checkpoint.json` trên Drive (không mất) và tự bỏ qua phần đã xong, không gọi lại
  API thừa.
- Muốn đỡ rủi ro ngắt giữa chừng: chia nhỏ theo khoảng thời gian, chạy nhiều lần Cell 4 với
  `--from`/`--to` hẹp hơn (vẫn cùng `--out`, cùng `--resume`).

**Dấu hiệu lỗi cần biết** (xem thêm bảng "Rủi ro cần canh" trong `WORKPLAN.md`):

| Thấy gì | Nghĩa là gì | Làm gì |
|---|---|---|
| `401 Invalid API key` | Key mới tạo chưa active | Dừng, chờ 10 phút – 2 tiếng, `--resume` chạy lại — đừng sửa code |
| Job đứng im lâu không log | Có thể đang throttle bình thường (1 call/giây) | Đợi, kiểm tra ô output có tăng số trạm không |
| Colab hiện "session crashed" | Hết RAM hoặc bị ngắt idle | `Runtime → Reconnect`, chạy lại từ Cell 1, `--resume` tiếp tục |

---

## Bước 9 — Chạy Cell 5: Pha 1 → 2 → 3

3 cell riêng, chạy tuần tự:

```bash
!PYTHONPATH=spark python spark/jobs/phase1_clean.py --input {DATA}/raw   --output {DATA}/clean
!PYTHONPATH=spark python spark/jobs/phase2_aqi.py   --input {DATA}/clean --output {DATA}/aqi --comparison-output docs/
!PYTHONPATH=spark python spark/jobs/phase3_aggregate.py --input {DATA}/aqi --output {DATA}/agg
```

Không cần đợi Cell 4 xong 100% mới chạy Cell 5 — Pha 1 đọc được bao nhiêu dữ liệu raw đã ghi thì
xử lý bấy nhiêu, chạy lại sau khi backfill xong thêm để cập nhật `clean/`, `aqi/`, `agg/`.

---

## Bước 10 — Chạy Cell 6: lấy số liệu điền báo cáo

In dung lượng từng tầng (`raw/clean/aqi/agg`). Ghi các số này (cùng tổng bản ghi, tổng call, thời
gian chạy — Cell 4 đã in ở cuối) vào **`docs/experiments.md` §1** trên máy Mac, commit lại.

---

## Bước 11 — Bàn giao dữ liệu

1. Trong Google Drive, chuột phải thư mục `MyDrive/big-data-aqi/air-quality/` → **Share** → thêm
   email của người kia (Người B) với quyền **Viewer** (hoặc Editor nếu cần chạy tiếp trên Colab
   của họ) — khỏi phải backfill lại.
2. Báo Người B đường dẫn Drive để họ tiếp tục Pha mở rộng (clustering/forecast) trên dữ liệu thật
   thay vì `data/samples/`.
3. Cập nhật `docs/experiments.md` và commit lên nhánh, mở PR như quy ước trong `WORKPLAN.md`.

---

## Xử lý sự cố nhanh

| Vấn đề | Nguyên nhân thường gặp | Cách xử lý |
|---|---|---|
| `ModuleNotFoundError` khi chạy Pha 1/2/3 | Chưa `PYTHONPATH=spark` hoặc Cell 1 chưa chạy xong | Chạy lại đúng lệnh có `PYTHONPATH=spark`, kiểm tra Cell 1 không báo lỗi |
| Pha 1/2/3 chạy nhưng output trống | `{DATA}/raw` chưa có file (Cell 4 chưa chạy hoặc bị ngắt sớm) | Kiểm tra `!ls {DATA}/raw/ingest_mode=history` có file `.jsonl.gz` chưa |
| `java.lang.UnsupportedClassVersionError` | Java không phải bản 17 | Chạy lại Cell 1b, kiểm tra `!java -version` ra đúng 17 trước khi chạy Pha 1 |
| Drive báo hết dung lượng | Free Drive 15 GB, dữ liệu nén `.jsonl.gz` chỉ vài trăm MB nhưng Drive có sẵn nhiều file khác | Dọn bớt Drive, hoặc backfill ít trạm hơn bằng `--limit` |
| Notebook mở lại thấy Cell 3 báo secret rỗng | Secrets là theo **trình duyệt/tài khoản**, không theo notebook | Mở lại panel 🔑, xác nhận `OWM_API_KEY` vẫn còn và Notebook access vẫn bật |
