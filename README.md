# خط أنابيب معالجة بيانات القرآن الكريم
تطبيق بايثون لجمع ومعالجة والتحقق من صحة وتصدير بيانات القرآن الكريم من واجهة برمجة التطبيقات من [Al Quran Cloud](https://alquran.cloud/api) مصمم بلغة بايثون لجمع البيانات القرآنية والتحقق من سلامتها وتوحيد تنسيقها وتصديرها بعدة صيغ.
<div style="display: flex; justify-content: center;">
  <a href="https://github.com/engsaleh/quran_pipeline/blob/main/SCREENSHOTS/خط%20أنابيب%20بيانات%20النصوص%20القرآنية.JPG?raw=true">
    <img src="https://github.com/engsaleh/quran_pipeline/blob/main/SCREENSHOTS/خط%20أنابيب%20بيانات%20النصوص%20القرآنية.JPG?raw=true" alt="خط معالجة النصوص القرآنية" width="700"/>
  </a>
</div>

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

### التنفيذ والخرج

لتشغيل الخط وإنشاء ملفات البيانات كل ما عليك هو تشغيل ملف `quran_pipeline.py` من الطرفية من خلال الأمر التالي:

```bash
python quran_pipeline.py
```
يستغرق التنفيذ بعض الوقت وبعد الانتهاء يتم إنشاء مجلد الخرج quran_output/ وبداخله ملفات الخرج الموضحة في الصورة التالية:
<div style="display: flex; justify-content: center;">
  <a href="https://github.com/engsaleh/quran_pipeline/blob/main/SCREENSHOTS/مخرجات%20خط%20الأنابيب.JPG?raw=true">
    <img src="https://github.com/engsaleh/quran_pipeline/blob/main/SCREENSHOTS/مخرجات%20خط%20الأنابيب.JPG?raw=true" alt="pipeline-output" width="700"/>
  </a>
</div>


## الترخيص
هذا المشروع مرخص بموجب رخصة MIT
BY: OLA SALEH




