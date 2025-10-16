# خط أنابيب معالجة بيانات القرآن الكريم
تطبيق بايثون لجمع ومعالجة والتحقق من صحة وتصدير بيانات القرآن الكريم من واجهة برمجة التطبيقات من [Al Quran Cloud](https://alquran.cloud/api) مصمم بلغة بايثون لجمع البيانات القرآنية والتحقق من سلامتها وتوحيد تنسيقها وتصديرها بعدة صيغ.
<img src="https://github.com/engsaleh/quran_pipeline/blob/main/SCREENSHOTS/%D8%AE%D8%B7%20%D8%A3%D9%86%D8%A7%D8%A8%D9%8A%D8%A8%20%D8%A8%D9%8A%D8%A7%D9%86%D8%A7%D8%AA20%D8%A7%D9%84%D9%86%D8%B5%D9%88%D8%B5%20%D8%A7%D9%84%D9%82%D8%B1%D8%A2%D9%86%D9%8A%D8%A9.JPG?raw=true" alt="خط معالجة النصوص القرآنية" width="500"/>


## الميزات الرئيسية

*   يجلب معلومات السور ونصوص الآيات بشكل غير متزامن.
*   يعالج النصوص القرآنية: ينظفها ويوحد أشكالها ويزيل التشكيل منها.
*    يتحقق من اكتمال وجودة البيانات بالمقارنة مع الإحصائيات الرسمية للقرآن الكريم
*   يوفر تنسيقات متعددة عند تصدير البيانات: تشمل قاعدة بيانات SQLite، وملفات JSON (للنصوص المبسطة وبالرسم العثماني)، وملف إحصائيات تفصيلية
*  يعالج الأخطاء ويعيد المحاولة باستخدام التراجع الأسي.

### المتطلبات

*   Python 3.8+

### التثبيت

1.  **قم باستنساخ المستودع:**
    ```bash
    git clone https://github.com/اسم_المستخدم_الخاص_بك/quran-data-pipeline.git
    cd quran-data-pipeline
    ```
2.  **أنشئ بيئة افتراضية (خطوة اختيارية مفضلة):**
    ```bash
    python -m venv venv
    source venv/bin/activate # على ويندوز: `venv\Scripts\activate`
    ```
3.  **قم بثبيت التبعيات المطلوبة:**
    ```bash
    pip install -r requirements.txt
    ```

### التنفيذ والمخرجات

لتشغيل الخط وإنشاء ملفات البيانات كل ما عليك هو تشغيل ملف `quran_pipeline.py` من الطرفية من خلال الأمر التالي:

```bash
python quran_pipeline.py
```

سيتم إنشاء مجلد الخرج quran_output/ الذي يحتوي على الملفات التالية:
* quran_database.sqlite
* quran_complete.json
* quran_simple.json
* quran_statistics.json
* quran_pipeline.log.

<a href="https://example.com/quran.png">
  <img src="https://github.com/engsaleh/quran_pipeline/blob/main/SCREENSHOTS/%D9%85%D8%AE%D8%B1%D8%AC%D8%A7%D8%AA%20%D8%AE%D8%B7%20%D8%A7%D9%84%D8%A3%D9%86%D8%A7%D8%A8%D9%8A%D8%A8.JPG?raw=true" alt="pipeline-output" width="500"/>
</a>
## الترخيص

هذا المشروع مرخص بموجب رخصة MIT
BY: OLA SALEH




