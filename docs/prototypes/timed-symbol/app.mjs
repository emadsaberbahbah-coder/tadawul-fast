import { SymbolSession, SessionTicker } from "./state.mjs";
import { INTEGRATION_CONFIG, CURRENT_REPORT_URLS, buildResearchUrl } from "./integration.mjs";

const PAGES = Object.freeze({
  overview: {
    title: "نظرة عامة على الرمز", english: "Symbol Overview",
    purpose: "ابدأ بقرار المحرك والعوامل والمخاطر، ثم انتقل إلى تفاصيل الرمز نفسه.",
    read: "اقرأ قرار المحرك وسبب المنع قبل التقديرات. تقويم الأرباح والتوزيعات يستخدم تاريخ اللقطة الظاهر.",
    limits: "BUY أو INVEST رأي في اللقطة، وليس تصريحًا بتنفيذ صفقة. عمر صف المصدر ليس عمر آخر سعر.",
    charts: [
      ["Engine Decision & Evidence — قرار المحرك والأدلة", "يجمع قرار المحرك والعوامل والمخاطر وسبب المنع. درجات الجودة والموثوقية من 100 ليست نسبة نجاح مثبتة."],
      ["Earnings & Dividend Calendar — تقويم الأرباح والتوزيعات", "الأيام المتبقية محسوبة من تاريخ لقطة التقويم. الموعد الناقص يعني أنه غير متاح للمصدر، ولا ينفي وجود حدث."],
    ],
  },
  fundamentals: {
    title: "البيانات المالية والتقييم", english: "Fundamentals & Valuation",
    purpose: "افهم حجم الشركة ونموها وربحيتها وديونها، ثم قارن مضاعفات التقييم.",
    read: "TTM آخر 12 شهرًا، وYoY مقارنة بالسنة السابقة، وbn مليار. مضاعفات P/E وP/B وP/S بوحدة ×.",
    limits: "المجاميع تحتفظ بوحدات المصدر. Quote CCY عملة السهم؛ لا تفترض أنها عملة القوائم المالية. الفراغ ليس صفرًا.",
    charts: [
      ["Business Fundamentals — مؤشرات الأداء المالي", "اقرأ الإيرادات والنمو والهوامش والتدفق النقدي الحر معًا. النمو الإيجابي وحده لا يثبت جودة الاستثمار."],
      ["Valuation & Income — التقييم والتوزيعات", "P/E وP/B وP/S وEV/EBITDA مضاعفات وليست عوائد. EPS ربحية السهم. نسبة التوزيع لا تضمن توزيعات مستقبلية."],
    ],
  },
  risk: {
    title: "المخاطر والمؤشرات الفنية", english: "Risk & Technical Indicators",
    purpose: "ضع حركة السعر في سياق التذبذب والخسائر التاريخية ونشاط التداول.",
    read: "RSI من 0 إلى 100. Beta حساسية للمرجع، وSharpe مقياس عائد معدل بالمخاطر. راجع فترة كل مقياس ووحدته.",
    limits: "Drawdown تاريخي. VaR تقدير قد تتجاوزه الخسارة الفعلية. يحتفظ الجدول بإشارة المصدر؛ الموجب في مقياس خسارة لا يعني ربحًا.",
    charts: [
      ["Volatility / Drawdown / VaR — التذبذب والتراجع والخطر المقدّر", "التذبذب يصف مقدار تغير السعر، والتراجع يقيس الهبوط من قمة إلى قاع. لا يمثل أي منهما حدًا مضمونًا للخسارة القادمة."],
      ["Beta / RSI / Sharpe — بيتا والزخم والعائد المعدل بالمخاطر", "لا يتحول أي مؤشر منفرد إلى أمر شراء أو بيع. اختلاف الفترات والمراجع مهم عند المقارنة."],
      ["52-Week Range & Volume — النطاق السنوي وحجم التداول", "52W position موقع نسبي داخل النطاق وليس توقعًا للسعر. Volume ومتوسطه كميات أسهم، وليسا مبالغ مالية."],
    ],
  },
  price: {
    title: "الرسوم السعرية", english: "Candlestick · SMA · Volume",
    purpose: "اقرأ شكل الحركة السعرية واتجاهها التاريخي وحجم التداول خلال الفترة المختارة.",
    read: "الشموع OHLC تمثل الافتتاح والأعلى والأدنى والإغلاق. SMA 20/50/200 متوسطات بسيطة بعدد جلسات التداول.",
    limits: "الشموع النسبية هنا % مقابل آخر إغلاق، وليست أسعارًا بالعملة. المتوسطات تلخص الماضي ولا تتنبأ بالجلسة التالية.",
    charts: [
      ["Relative Candlestick Chart (OHLC) — الشموع النسبية", "المحور الرأسي نسبة مقارنة بآخر إغلاق متاح. يعرض الرسم حتى 130 جلسة ضمن البيانات المتاحة؛ اختر نطاقًا أقصر لرؤية التفاصيل."],
      ["Close & SMA 20 / 50 / 200 — الإغلاق والمتوسطات البسيطة", "تقارن الخطوط الإغلاق بمتوسطات 20 و50 و200 جلسة. الأسعار بوحدة عملة السهم؛ التقاطعات إشارات للمراجعة وليست ضمانًا للاتجاه."],
      ["Volume & 20-Session Average — الحجم ومتوسط 20 جلسة", "الأعمدة عدد الأسهم المتداولة والخط متوسط الحجم. ارتفاع الحجم وحده لا يحدد اتجاه السعر."],
    ],
  },
  ratings: {
    title: "التقييم والأدلة", english: "TFB Evidence Score & Analyst Consensus",
    purpose: "افصل تقييم TFB الخاص عن آراء المحللين، وافهم المكونات المتاحة لكل منهما.",
    read: "وزن الأدلة المتاح يقيس اكتمال المكونات. إجماع المحللين على مقياس 1–5؛ تغيره بالنقاط مقابل لقطة المقارنة.",
    limits: "الثقة تصنيف منهجي وليست احتمال نجاح. عدم توافر محللين لا يعني رأيًا محايدًا. none في قيد التقييم لا يثبت السماح بالتنفيذ.",
    charts: [
      ["TFB Evidence Score & Components — درجة TFB ومكوناتها", "هذا تقييم خاص مبني على المكونات المتاحة. اقرأ درجة كل جزء وقيود المحرك معه؛ لا تعتبر النتيجة تصريحًا بالتداول."],
      ["Analyst Consensus — إجماع المحللين", "راجع عدد المحللين وتاريخ اللقطة وتاريخ المقارنة؛ قد يكون الأخير أقدم من 28 يومًا. الهدف السعري ليس عائدًا محققًا."],
    ],
  },
  news: {
    title: "الأخبار والسجل التاريخي", english: "News & Events · Signal / Board History",
    purpose: "راجع الخبر الأصلي وسياقه، ثم تابع تغير قرار المحرك وحلقات لوحة البحث.",
    read: "افتح المصدر وراجع ارتباط الخبر بالشركة ووقت نشره. ميّز تاريخ النشر عن أول رصد، والنبرة الآلية عن الحدث المؤكد.",
    limits: "التغطية جزئية. غياب الصفوف لا يثبت عدم وجود أخبار. النبرة الإيجابية لا تثبت صحة الخبر ولا تضمن ارتفاع السعر.",
    charts: [
      ["News & Events — الأخبار والأحداث", "قد يأتي التاريخ من النشر أو أول الرصد عند غيابه. أهمية الحدث والوسوم والنبرة متاحة بحسب المصدر، وقد يتكرر الحدث عبر عدة ناشرين."],
      ["Signal History — سجل قرارات المحرك", "كل صف رأي بحثي مسجل في وقته، وليس إثباتًا لشراء أو بيع فعلي. قد تمنع قيود أخرى تنفيذ الرأي."],
      ["Board Episode History — سجل حلقات اللوحة", "+5 و+20 تعني جلسات تداول من سعر الدخول، لا أيامًا تقويمية. الفراغ قد يعني أن المدة لم تكتمل أو أن النتيجة غير متاحة."],
    ],
  },
});

const $ = id => document.getElementById(id);
let storage;
try { storage = window.sessionStorage; } catch { /* In-memory operation remains available. */ }
const session = new SymbolSession({ storage });
let renderedPage = null;
let renderedEmbed = null;
let renderedSnapshot = null;

function announce(text) { $("announcements").textContent = text; }

function showHelp() {
  const page = PAGES[session.snapshot().activePage];
  $("helpTitle").textContent = `كيف أقرأ: ${page.title}؟`;
  const body = $("helpBody");
  body.replaceChildren();
  for (const [title, text] of page.charts) {
    const heading = document.createElement("h3");
    heading.textContent = title;
    const paragraph = document.createElement("p");
    paragraph.textContent = text;
    body.append(heading, paragraph);
  }
  $("helpDialog").showModal();
}

function updateEmbed(snapshot) {
  const result = buildResearchUrl(INTEGRATION_CONFIG, snapshot.activePage, snapshot.effectiveSymbol);
  $("currentReport").href = CURRENT_REPORT_URLS[snapshot.activePage];
  $("dataSectionTag").textContent = PAGES[snapshot.activePage].english;
  $("dataPlaceholder").hidden = result.connected;
  $("embedSection").hidden = !result.connected;
  if (!result.connected) {
    // No silent fallback to an unfiltered or previously selected report.
    $("embedHost").replaceChildren();
    renderedEmbed = null;
    return;
  }
  $("sentSymbol").textContent = result.symbol;
  if (renderedEmbed === result.url) return; // Never reload on countdown ticks.
  const iframe = document.createElement("iframe");
  iframe.title = `${PAGES[snapshot.activePage].title} — ${result.symbol}`;
  iframe.referrerPolicy = "no-referrer";
  iframe.loading = "eager";
  iframe.src = result.url;
  $("embedHost").replaceChildren(iframe);
  renderedEmbed = result.url;
}

function render(snapshot, expiredPages = []) {
  const page = PAGES[snapshot.activePage];
  $("masterBadge").textContent = snapshot.masterSymbol;
  $("effectiveSymbol").textContent = snapshot.effectiveSymbol;
  $("selectionStatus").textContent = snapshot.isOverride ? "مقارنة مؤقتة لهذه الصفحة" : "يتبع الرمز الأساسي";
  $("selectionStatus").classList.toggle("temporary", snapshot.isOverride);
  $("countdown").hidden = !snapshot.isOverride;
  const seconds = Math.ceil(snapshot.remainingMs / 1000);
  $("countdown").textContent = `${String(Math.floor(seconds / 60)).padStart(2,"0")}:${String(seconds % 60).padStart(2,"0")}`;
  $("masterSection").hidden = snapshot.activePage !== "overview";
  $("overrideSection").hidden = snapshot.activePage === "overview";
  $("clearOverride").disabled = !snapshot.isOverride;
  if (renderedPage !== snapshot.activePage) {
    const heading = document.createElement("span");
    heading.textContent = page.title;
    const english = document.createElement("small");
    english.textContent = page.english;
    $("pageTitle").replaceChildren(heading, english);
    $("pagePurpose").textContent = page.purpose;
    $("helpPurpose").textContent = page.purpose;
    $("helpRead").textContent = page.read;
    $("helpLimits").textContent = page.limits;
    $("overrideInput").value = snapshot.overrides[snapshot.activePage]?.symbol ?? "";
    for (const button of $("navigation").querySelectorAll("button")) {
      if (button.dataset.page === snapshot.activePage) button.setAttribute("aria-current", "page");
      else button.removeAttribute("aria-current");
    }
    renderedPage = snapshot.activePage;
  }
  // Countdown renders must not overwrite a draft after the viewer tabs to Apply.
  if (renderedSnapshot?.masterSymbol !== snapshot.masterSymbol) {
    $("masterInput").value = snapshot.masterSymbol;
  }
  if (expiredPages.includes(snapshot.activePage) && !snapshot.isOverride) {
    const draft = $("overrideInput").value.trim().toUpperCase();
    if (renderedSnapshot?.activePage === snapshot.activePage &&
        draft === renderedSnapshot.effectiveSymbol) $("overrideInput").value = "";
    announce(`انتهت المقارنة المؤقتة. عاد هذا القسم إلى الرمز الأساسي ${snapshot.masterSymbol}.`);
  }
  updateEmbed(snapshot);
  renderedSnapshot = snapshot;
}

const ticker = new SessionTicker(session, render);
function navigate(page) {
  session.setPage(page);
  $("formError").textContent = "";
  ticker.focus();
}

for (const [id, page] of Object.entries(PAGES)) {
  const button = document.createElement("button");
  button.type = "button";
  button.dataset.page = id;
  button.textContent = page.title;
  const english = document.createElement("small");
  english.textContent = page.english;
  button.append(english);
  button.addEventListener("click", () => navigate(id));
  $("navigation").append(button);
}

$("masterForm").addEventListener("submit", event => {
  event.preventDefault();
  try {
    const result = session.setMaster($("masterInput").value);
    $("masterInput").value = result.masterSymbol;
    $("formError").textContent = "";
    ticker.focus();
    announce(`الرمز الأساسي الآن ${result.masterSymbol}. ألغيت المقارنات المؤقتة في جميع الأقسام.`);
  } catch (error) { $("formError").textContent = error.message; }
});
$("overrideForm").addEventListener("submit", event => {
  event.preventDefault();
  try {
    const result = session.setOverride(session.snapshot().activePage, $("overrideInput").value);
    $("formError").textContent = "";
    ticker.focus();
    announce(result.isOverride ? `استخدم ${result.effectiveSymbol} لهذه الصفحة لخمس دقائق.` : "عاد هذا القسم إلى الرمز الأساسي.");
  } catch (error) { $("formError").textContent = error.message; }
});
$("clearOverride").addEventListener("click", () => {
  session.clearOverride();
  $("overrideInput").value = "";
  ticker.focus();
  announce("عاد هذا القسم إلى الرمز الأساسي.");
});
$("goOverview").addEventListener("click", () => navigate("overview"));
$("openHelp").addEventListener("click", showHelp);
$("closeHelp").addEventListener("click", () => $("helpDialog").close());
window.addEventListener("focus", () => ticker.focus());
window.addEventListener("pageshow", () => ticker.focus());
document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "visible") ticker.focus();
});
window.addEventListener("pagehide", () => ticker.stop());
window.addEventListener("pageshow", () => ticker.start());
ticker.start();
