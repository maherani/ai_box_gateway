# جعبه پردازش هوشمند اسناد (AI Box System)
## موتور مستقل برداری‌سازی، نمایه‌سازی و پردازش معنایی اسناد (System of Intelligence)

**محل استقرار مخزن:** `/home/alborz/ai-box`  
**فناوری‌های پایه:** Python 3.10+, Docker Compose, AnythingLLM, LanceDB, Ollama, SQLite, RabbitMQ  
**سرویس لینوکسی:** `ai-box-consumer.service`  
**وضعیت:** عملیاتی، ماژولارشده با آداپتور تعویض‌پذیر (Plug-and-Play) و آزموده با ۱۳۱ آزمون خودکار (۱۰۰٪ قبولی)  

---

## ۱. نقش و مرزهای معماری (System of Intelligence)
سامانه **AI Box** محیط پردازش هوشمند و استنتاج معنایی اسناد آرشیو سازمانی است. این سامانه فاقد هرگونه حق تغییر روی فایل‌های اصلی یا دسترسی مستقیم به سیستم‌فایل آرشیو است. فایل‌ها منحصراً از طریق صف‌های پیام RabbitMQ و پراکسی استریمینگ لایه میانی (`:8090`) دریافت می‌شوند و خروجی چت هوشمند از طریق درگاه امن گیت‌وی به کاربران پرتال ارائه می‌گردد.

---

## ۲. ساختار سرویس‌ها و کانتینرهای داکر
این سامانه بر پایه ۴ کانتینر در یک شبکه مجزا و ایزوله داکر (`ai_box_net`) فعالیت می‌کند:

```
[پرتال آرشیو / کاربر] ---> http://127.0.0.1:3001 (پورت منتشرشده روی هاست)
                                   │
                                   ▼
                        ┌─────────────────────┐
                        │   ai_box_gateway    │ (احراز نشست، بررسی ACL، پاکسازی کوکی، اعتبارسنجی فایل)
                        └──────────┬──────────┘
                                   │
        ┌──────────────────────────┴──────────────────────────┐
        ▼ (شبیه‌ساز پیش‌فرض AnythingLLM)                         ▼ (سرور بیرونی با تغییر .env)
┌───────────────────────┐                             ┌──────────────────────────────┐
│  ai_box_anythingllm   │                             │  External AI Box Appliance   │
│ (LanceDB Vector DB)   │                             │  (Production AI Engine)      │
└──────────┬────────────┘                             └──────────────────────────────┘
           │ (http://ollama:11434)
           ▼
┌───────────────────────┐
│     ai_box_ollama     │
│ (Local LLM & Embed)   │
└───────────────────────┘
```

| نام کانتینر | تصویر داکر / منبع | نقش و دسترسی شبکه |
| :--- | :--- | :--- |
| `ai_box_gateway` | ایمیج بومی مبتنی بر پایتون ۳.۱۰ | درگاه ورود امن چت و کنترل دسترسی (تنها پورت عمومی `127.0.0.1:3001`) |
| `ai_box_anythingllm` | `mintplexlabs/anythingllm` | شبیه‌ساز محلی RAG و پایگاه داده برداری LanceDB (شبکه داخلی) |
| `ai_box_ollama` | `ollama/ollama` | سرور محلی استنتاج مدل‌های `qwen2.5:0.5b-instruct` و `bge-m3` |
| `ai_box_rabbitmq` | `rabbitmq:3-management` | کارگزار صف پیام (پورت‌های ۵۶۷۲ و ۱۵۶۷۲ برای صف `ai_box.file_events`) |

---

## ۳. درگاه امن هوش مصنوعی (AI Box Gateway)
فایل `gateway/proxy.py` وظایف امنیتی و مدیریت ترافیک حیاتی زیر را بر عهده دارد:
1. **معماری کشینگ و همگام‌سازی مبتنی بر نشست (Session-based ACL Caching):**
   - **واکشی در زمان ورود (Login-time Sync):** مجوزهای دسترسی کاربر به فضاهای کاری در اولین تعامل نشست از اندپوینت `/api/v1/acl/workspaces` واکشی می‌شود.
   - **حافظه نهان چندنخی امن (Thread-Safe In-Memory Cache):** مجوزها با هش امن SHA-256 توکن در ماژول `SessionACLCache` ذخیره می‌گردند.
   - **حذف کامل سرریز شبکه در چت (Zero Per-Request Network Overhead):** پیام‌های چت و استریمینگ چت بدون استعلام مجدد شبکه با تکیه بر کش نشست در کسر میلی‌ثانیه هدایت می‌شوند.
   - **ابطال خودکار در خروج (Invalidation on Logout):** با خروج کاربر یا ارسال درخواست به `/api/v1/session/invalidate`، نشست بلافاصله از کش ابطال می‌گردد.
2. **بررسی پویای سطوح دسترسی پوشه‌ها (Dynamic Folder ACL):** فضاهای مجاز کاربر (شامل `folder-{id}` و `personal-{uid}`) تفکیک شده و تلاش برای نفوذ به فضاهای غیرمجاز با خطای `403 Forbidden` مسدود می‌شود.
3. **پاکسازی کوکی‌ها (Cookie Stripping):** کوکی‌های احراز هویت Nextcloud پیش از فوروارد حذف شده و درخواست با کلید امنیتی سرویس داخلی (`INTERNAL_SERVICE_KEY`) امضا و ارسال می‌شود.
4. **حفاظت در برابر جعل دامنه (CORS/Origin Guard):** درخواست‌های Cross-Origin نامعتبر را به طور کامل رد می‌کند.
5. **پشتیبانی از استریمینگ داده (SSE Streaming & JSON):** پاسخ‌های زنده جریانی (`text/event-stream`) و بسته‌های استاندارد JSON را با پایپ‌لاین امن تحویل کلاینت می‌دهد.

---

## ۴. پردازشگر رویدادها و پایگاه داده وظایف (Task Consumer & Task DB)
سرویس پس‌زمینه `connector/consumer.py` به عنوان یک سرویس لینوکسی پیوسته اجرا می‌شود:
* **دریافت پیام از صف:** رویدادهای `file.created`، `file.deleted` و `file.moved` را از صف `ai_box.file_events` کارگزار RabbitMQ تحویل می‌گیرد.
* **ثبت در پایگاه داده وظایف (`ai_box_tasks.db`):** پیام بلافاصله در دیتابیس محلی SQLite با شناسه لیس (Lease) و وضعیت `PENDING` ثبت شده و سپس به RabbitMQ تأییدیه ACK داده می‌شود.
* **واکشی بدون دیسک:** محتوای سند از پراکسی لایه میانی (`:8090/files/{fileId}`) به صورت استریم بایت‌ها دریافت می‌شود.
* **تحویل به آداپتور مقصد:** سند همراه با نام و شناسه فضای کاری (`folder-{id}` یا `personal-{uid}`) به آداپتور موتور هوش مصنوعی سپرده می‌شود.

---

## ۵. آداپتور ماژولار تعویض‌پذیر (Plug-and-Play Engine Adapter)
طراحی ماژولار در `connector/destination_adapter.py` بر پایه معماری شی‌گرا و رابط انتزاعی پیاده‌سازی شده است:

```
                  ┌──────────────────────┐
                  │ BaseAIBackendEngine  │ (رابط انتزاعی قراردادها)
                  └──────────┬───────────┘
                             │
            ┌────────────────┴────────────────┐
            ▼                                 ▼
┌─────────────────────────┐       ┌───────────────────────────────┐
│ AnythingLLMEngineAdapter│       │  ExternalAIBoxEngineAdapter   │
│ (شبیه‌ساز محلی فاز MVP)   │       │  (موتور تولیدی سرور بیرونی)   │
└─────────────────────────┘       └───────────────────────────────┘
```

### نحوه تعویض شبیه‌ساز با سرور هوش مصنوعی واقعی (Production Swap):
بدون هیچ‌گونه دستکاری در کد برنامه، صرفاً فایل پیکربندی `~/ai-box/.env` (برگرفته از `.env.example`) را تنظیم نمایید:

```ini
# ۱. تغییر ارائه‌دهنده به جعبه بیرونی
AI_BACKEND_PROVIDER=external_ai_box

# ۲. مشخصات آدرس و پورت سرور واقعی هوش مصنوعی
BACKEND_HOST=192.168.10.50
BACKEND_PORT=8000
AI_BACKEND_PROTOCOL=http

# ۳. کلید امنیتی مشترک جهت احراز هویت
INTERNAL_SERVICE_KEY=your_secure_shared_token_min_16_chars
DESTINATION_API_KEY=your_secure_shared_token_min_16_chars
```

سپس سرویس را مجدداً راه‌اندازی کنید:
```bash
sudo systemctl restart ai-box-consumer.service
docker compose restart gateway
```
مستند رسمی و قرارداد فنی متناظر در [AI_BOX_INTEGRATION_CONTRACT.md](file:///home/alborz/AI_BOX_INTEGRATION_CONTRACT.md) تشریح شده است.

---

## ۶. دستورات مدیریتی و پایش سیستم

### الف) مدیریت کانتینرهای داکر:
```bash
cd /home/alborz/ai-box

# اجرای سرویس‌ها:
docker compose up -d

# بازسازی کانتینر گیت‌وی پس از تغییرات:
docker compose up -d --build gateway

# بررسی وضعیت سلامت:
docker compose ps

# مشاهده لاگ درگاه گیت‌وی:
docker compose logs -f gateway

# مشاهده لاگ کارگزار پیام:
docker compose logs -f rabbitmq
```

### ب) مدیریت سرویس کانسومر هوش مصنوعی (systemd):
```bash
# بررسی وضعیت سرویس:
sudo systemctl status ai-box-consumer.service

# راه‌اندازی مجدد سرویس:
sudo systemctl restart ai-box-consumer.service

# مشاهده لاگ‌های زنده کانسومر:
journalctl -u ai-box-consumer.service -f
```

---

## ۷. آزمون‌های خودکار و تضمین کیفیت (Unit & Adapter Tests)
مجموعه ۱۳۱ آزمون خودکار شامل شبیه‌سازی کامل درخواست‌های چت، تست‌های آداپتور خارجی، بررسی اعتبارسنجی نشست‌ها و ممیزی وظایف دیتابیس:

```bash
cd /home/alborz/ai-box/connector
python3 -m unittest discover -s tests -p 'test_*.py'
```
تمام ۱۳۱ آزمون با موفقیت ۱۰۰٪ پاس می‌شوند.
