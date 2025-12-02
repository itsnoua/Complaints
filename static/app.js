// static/app.js

let visitsChart = null;

// ميتاداتا القطاعات (label + البلديات)
let sectorMeta = null;

// حالة الفلاتر في الصفحة الرئيسية
const currentFilter = {
  sector: null,        // "abha", "khamis" ...
  municipality: null,  // اسم بلدية بالعربي
};

// هل الصفحة قطاع مخصص (abha، khamis... الخ)؟
function getBodySector() {
  const body = document.body;
  if (!body) return null;
  const s = body.getAttribute("data-sector");
  return s || null;
}

// القطاع الذي سيستخدم في API (أولوية لصفحات القطاعات)
function getSectorKey() {
  const bodySector = getBodySector();
  if (bodySector) return bodySector;
  return currentFilter.sector;
}

// بلدية الفلتر (فقط في الصفحة الرئيسية)
function getMunicipalityFilter() {
  const bodySector = getBodySector();
  if (bodySector) return null; // صفحات القطاعات لا تستخدم فلتر بلدية
  return currentFilter.municipality;
}

// تنقّل بين الصفحات
function navigateTo(target) {
  if (target === "home") {
    window.location.href = "/";
  } else {
    window.location.href = "/" + target + ".html";
  }
}

// إعداد القائمة الجانبية
function setupMenu() {
  const menuBtn = document.getElementById("menu-btn");
  const sideMenu = document.getElementById("side-menu");
  if (!menuBtn || !sideMenu) return;

  menuBtn.addEventListener("click", (e) => {
    e.stopPropagation();
    sideMenu.classList.toggle("open");
  });

  sideMenu.addEventListener("click", (e) => {
    e.stopPropagation();
  });

  document.addEventListener("click", () => {
    if (sideMenu.classList.contains("open")) {
      sideMenu.classList.remove("open");
    }
  });
}

// تحديث زر تحميل القطاع حسب الفلتر
function updateSectorDownloadButton() {
  const container = document.getElementById("download-sector-container");
  const btn = document.getElementById("download-sector-btn");
  if (!container || !btn) return;

  // لو صفحة قطاعية (abha.html ..) لا نعرض الزر
  if (getBodySector()) {
    container.style.display = "none";
    return;
  }

  const sectorKey = currentFilter.sector;
  if (!sectorKey) {
    // لا يوجد قطاع مختار
    container.style.display = "none";
    return;
  }

  const label =
    sectorMeta &&
    sectorMeta[sectorKey] &&
    sectorMeta[sectorKey].label
      ? sectorMeta[sectorKey].label
      : sectorKey;

  btn.textContent = "تحميل ملفات " + label;

  btn.onclick = () => {
    // يطلب من الخادم ملف zip
    window.location.href = `/api/download/sector/${sectorKey}`;
  };

  container.style.display = "block";
}

// إعداد فلاتر الصفحة الرئيسية
async function setupHomeFilters() {
  // لو الصفحة قطاعية (abha.html ..) لا نستخدم الفلاتر
  if (getBodySector()) return;

  const sectorSelect = document.getElementById("sector-filter");
  const muniSelect = document.getElementById("municipality-filter");
  if (!sectorSelect || !muniSelect) return;

  try {
    const resp = await fetch("/api/meta/sectors");
    if (!resp.ok) throw new Error("failed meta");
    sectorMeta = await resp.json();   // نخزنها عالمياً

    // تعبئة قائمة القطاعات
    Object.entries(sectorMeta).forEach(([key, info]) => {
      const opt = document.createElement("option");
      opt.value = key;
      opt.textContent = info.label || key;
      sectorSelect.appendChild(opt);
    });

    // تغيير القطاع
    sectorSelect.addEventListener("change", () => {
      const val = sectorSelect.value || "";
      if (!val) {
        // الكل
        currentFilter.sector = null;
        currentFilter.municipality = null;

        muniSelect.innerHTML = '<option value="">كل البلديات</option>';
        muniSelect.disabled = true;

        updateSectorDownloadButton();
        loadTotals();
        loadChart();
        return;
      }

      currentFilter.sector = val;
      currentFilter.municipality = null;

      // تعبئة البلديات حسب القطاع
      muniSelect.innerHTML = '<option value="">كل البلديات</option>';
      const info = sectorMeta[val];
      if (info && Array.isArray(info.municipalities)) {
        info.municipalities.forEach((m) => {
          const opt = document.createElement("option");
          opt.value = m;
          opt.textContent = m;
          muniSelect.appendChild(opt);
        });
      }
      muniSelect.disabled = false;

      updateSectorDownloadButton();
      loadTotals();
      loadChart();
    });

    // تغيير البلدية
    muniSelect.addEventListener("change", () => {
      const val = muniSelect.value || "";
      if (!val) {
        // كل البلديات داخل القطاع
        currentFilter.municipality = null;
      } else {
        currentFilter.municipality = val;
      }
      loadTotals();
      loadChart();
    });

    // بداية: لا يوجد قطاع محدد => إخفاء الزر
    updateSectorDownloadButton();
  } catch (e) {
    console.error("failed to setup filters", e);
  }
}

// تحميل الأرقام + الفروقات للبطاقات
async function loadTotals() {
  const statusDiv = document.getElementById("status");
  if (statusDiv) statusDiv.textContent = "جاري تحميل البيانات...";

  const bodySector = getBodySector();
  const filterSector = getSectorKey();
  const filterMuni = getMunicipalityFilter();

  let url = "/api/totals";

  if (bodySector) {
    // صفحة قطاعية
    url = `/api/totals/sector/${bodySector}`;
  } else if (filterMuni) {
    url = `/api/totals/municipality/${encodeURIComponent(filterMuni)}`;
  } else if (filterSector) {
    url = `/api/totals/sector/${filterSector}`;
  }

  try {
    const resp = await fetch(url);
    if (!resp.ok) {
      const err = await resp.json().catch(() => ({}));
      if (statusDiv) statusDiv.textContent = err.error || "لا توجد بيانات متاحة.";
      return;
    }

    const data = await resp.json();

    const visited = Number(data.visited || 0);
    const notVisited = Number(data.not_visited || 0);
    const total = Number(data.total || 0);

    const prevVisited = data.prev_visited !== null ? Number(data.prev_visited) : null;
    const prevNot = data.prev_not_visited !== null ? Number(data.prev_not_visited) : null;
    const prevTotal = data.prev_total !== null ? Number(data.prev_total) : null;

    const deltaVisited = data.delta_visited;
    const deltaNot = data.delta_not_visited;
    const deltaTotal = data.delta_total;

    const elVisited = document.getElementById("card-visited-value");
    const elNot = document.getElementById("card-not-visited-value");
    const elTotal = document.getElementById("card-total-value");

    if (elVisited) elVisited.textContent = visited.toString();
    if (elNot) elNot.textContent = notVisited.toString();
    if (elTotal) elTotal.textContent = total.toString();

    function updateDelta(el, prevVal, deltaVal) {
      if (!el) return;
      el.classList.remove("delta-positive", "delta-negative", "delta-neutral");

      if (prevVal === null || deltaVal === null) {
        el.textContent = "لا يوجد يوم سابق";
        el.classList.add("delta-neutral");
      } else if (deltaVal > 0) {
        el.textContent = `+${deltaVal} عن أمس`;
        el.classList.add("delta-positive");
      } else if (deltaVal < 0) {
        el.textContent = `${deltaVal} عن أمس`;
        el.classList.add("delta-negative");
      } else {
        el.textContent = "لا تغيير عن أمس";
        el.classList.add("delta-neutral");
      }
    }

    updateDelta(document.getElementById("card-not-visited-delta"), prevNot, deltaNot);
    updateDelta(document.getElementById("card-visited-delta"), prevVisited, deltaVisited);
    updateDelta(document.getElementById("card-total-delta"), prevTotal, deltaTotal);

    if (statusDiv) statusDiv.textContent = "";
  } catch (e) {
    console.error(e);
    if (statusDiv) statusDiv.textContent = "خطأ في الاتصال بالخادم.";
  }
}

// تحميل الرسم البياني
async function loadChart() {
  const canvas = document.getElementById("visitsChart");
  if (!canvas) return;

  const bodySector = getBodySector();
  const filterSector = getSectorKey();
  const filterMuni = getMunicipalityFilter();

  let url = "/api/chart-data";

  if (bodySector) {
    url = `/api/chart-data/sector/${bodySector}`;
  } else if (filterMuni) {
    url = `/api/chart-data/municipality/${encodeURIComponent(filterMuni)}`;
  } else if (filterSector) {
    url = `/api/chart-data/sector/${filterSector}`;
  }

  try {
    const resp = await fetch(url);
    if (!resp.ok) return;

    const data = await resp.json();
    const labels = data.labels || [];
    const current = data.current || [];
    const previous = data.previous || [];

    const ctx = canvas.getContext("2d");

    if (visitsChart) {
      visitsChart.destroy();
    }

    visitsChart = new Chart(ctx, {
      type: "bar",
      data: {
        labels: labels,
        datasets: [
          {
            label: "اليوم الحالي",
            data: current,
            backgroundColor: "#5e3b36"
          },
          {
            label: "اليوم السابق",
            data: previous,
            backgroundColor: "#9d7a72"
          }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: {
            position: "top",
            labels: {
              font: { family: "Tajawal" }
            }
          },
          tooltip: {
            rtl: true,
            bodyFont: { family: "Tajawal" },
            titleFont: { family: "Tajawal" }
          }
        },
        scales: {
          x: {
            ticks: {
              font: { family: "Tajawal" }
            }
          },
          y: {
            beginAtZero: true,
            ticks: {
              font: { family: "Tajawal" }
            }
          }
        }
      }
    });
  } catch (e) {
    console.error(e);
  }
}


// نموذج رفع الملفات (upload.html)
function setupUploadForm() {
  const form = document.getElementById("upload-form");
  if (!form) return;

  const statusDiv = document.getElementById("upload-status");

  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    if (statusDiv) statusDiv.textContent = "جاري رفع الملفات وتشغيل الأداة...";

    const formData = new FormData(form);

    try {
      const resp = await fetch("/api/process", {
        method: "POST",
        body: formData,
      });

      if (!resp.ok) {
        const err = await resp.json().catch(() => ({}));
        if (statusDiv) statusDiv.textContent = "خطأ: " + (err.error || resp.statusText);
        return;
      }

      await resp.json();
      if (statusDiv) statusDiv.textContent = "تمت المعالجة بنجاح. ارجع للصفحة الرئيسية أو صفحات القطاعات لمشاهدة التحديث.";
    } catch (err) {
      console.error(err);
      if (statusDiv) statusDiv.textContent = "خطأ في الاتصال بالخادم.";
    }
  });
}

document.addEventListener("DOMContentLoaded", () => {
  setupMenu();
  setupUploadForm();
  setupHomeFilters(); // الفلاتر للصفحة الرئيسية فقط
  loadTotals();
  loadChart();
});
