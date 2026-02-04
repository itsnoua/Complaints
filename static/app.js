// static/app.js

// helper to get stored creds
function getAuthHeader() {
  const creds = sessionStorage.getItem("auth_creds");
  return creds ? { "Authorization": creds } : {};
}

// Redirect if not logged in (unless we are on the login page)
if (!sessionStorage.getItem("auth_creds") && window.location.pathname !== "/" && window.location.pathname !== "/index.html") {
  window.location.href = "/";
}

// ==================== حالة عامة ====================
let visitsChart = null;
let sectorMeta = null;

const currentFilter = {
  sector: null,
  municipality: null,
};

// ==================== تنقّل الصفحات ====================
function navigateTo(page) {
  if (page === "home") {
    // If user clicks "Home" from menu, go to dashboard
    window.location.href = "/dashboard.html";
  } else {
    window.location.href = `/${page}.html`;
  }
}

// ==================== دوال مساعدة للفلترة ====================
function getBodySector() {
  const body = document.body;
  if (!body) return null;
  const s = body.getAttribute("data-sector");
  return s || null;
}

function getSectorKey() {
  const bodySector = getBodySector();
  if (bodySector) return bodySector;
  return currentFilter.sector;
}

function getMunicipalityFilter() {
  const bodySector = getBodySector();
  if (bodySector) return null;
  return currentFilter.municipality;
}

// ==================== القائمة الجانبية ====================
function setupMenu() {
  const menuBtn = document.getElementById("menu-btn");
  const sideMenu = document.getElementById("side-menu");
  const overlay = document.getElementById("menu-overlay"); // If these exist in old HTML

  if (!menuBtn || !sideMenu || !overlay) return;

  menuBtn.addEventListener("click", () => {
    sideMenu.classList.toggle("open");
    overlay.classList.toggle("active");
  });

  overlay.addEventListener("click", () => {
    sideMenu.classList.remove("open");
    overlay.classList.remove("active");
  });
}

// ==================== فلاتر الصفحة الرئيسية ====================
async function setupHomeFilters() {
  if (getBodySector()) return;

  const sectorSelect = document.getElementById("sector-filter");
  const muniSelect = document.getElementById("municipality-filter");
  if (!sectorSelect || !muniSelect) return;

  try {
    const resp = await fetch("/api/meta/sectors", { headers: getAuthHeader() });
    if (!resp.ok) throw new Error("failed meta");
    sectorMeta = await resp.json();

    Object.entries(sectorMeta).forEach(([key, info]) => {
      const opt = document.createElement("option");
      opt.value = key;
      opt.textContent = info.label || key;
      sectorSelect.appendChild(opt);
    });

    sectorSelect.addEventListener("change", () => {
      const val = sectorSelect.value || "";
      if (!val) {
        currentFilter.sector = null;
        currentFilter.municipality = null;
        muniSelect.innerHTML = '<option value="">كل البلديات</option>';
        muniSelect.disabled = true;
        loadMunicipalityDetails();
        loadTotals();
        loadChart();
        return;
      }
      currentFilter.sector = val;
      currentFilter.municipality = null;
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
      loadMunicipalityDetails();
      loadTotals();
      loadChart();
    });

    muniSelect.addEventListener("change", () => {
      const val = muniSelect.value || "";
      currentFilter.municipality = val || null;
      loadMunicipalityDetails();
      loadTotals();
      loadChart();
    });
  } catch (e) {
    console.error("failed to setup filters", e);
  }
}

// ==================== تحميل أرقام البطاقات ====================
async function loadTotals() {
  const statusDiv = document.getElementById("status");
  if (statusDiv) statusDiv.textContent = "جاري تحميل البيانات...";

  const bodySector = getBodySector();
  const filterSector = getSectorKey();
  const filterMuni = getMunicipalityFilter();

  let url = "/api/totals";
  if (bodySector) {
    url = `/api/totals/sector/${bodySector}`;
  } else if (filterMuni) {
    url = `/api/totals/municipality/${encodeURIComponent(filterMuni)}`;
  } else if (filterSector) {
    url = `/api/totals/sector/${filterSector}`;
  }

  try {
    const resp = await fetch(url, { headers: getAuthHeader() });
    if (!resp.ok) {
      if (statusDiv) statusDiv.textContent = "لا توجد بيانات متاحة.";
      return;
    }

    const data = await resp.json();
    const visited = Number(data.visited || 0);
    const notVisited = Number(data.not_visited || 0);
    const total = Number(data.total || 0);
    const prevVisited = data.prev_visited !== null ? Number(data.prev_visited) : null;
    const prevNot = data.prev_not_visited !== null ? Number(data.prev_not_visited) : null;
    const prevTotal = data.prev_total !== null ? Number(data.prev_total) : null;
    const prevDate = data.prev_run_date || null;

    const elVisited = document.getElementById("card-visited-value");
    const elNot = document.getElementById("card-not-visited-value");
    const elTotal = document.getElementById("card-total-value");

    if (elVisited) elVisited.textContent = visited.toString();
    if (elNot) elNot.textContent = notVisited.toString();
    if (elTotal) elTotal.textContent = total.toString();

    function updateDelta(el, prevVal, deltaVal, label) {
      if (!el) return;
      el.classList.remove("delta-positive", "delta-negative", "delta-neutral");
      if (prevVal === null || deltaVal === null || prevDate === null) {
        el.textContent = `لا يوجد تشغيل سابق لـ ${label}`;
        el.classList.add("delta-neutral");
      } else if (deltaVal > 0) {
        el.textContent = `+${deltaVal} مقارنةً بالتشغيل السابق (${prevDate})`;
        el.classList.add("delta-positive");
      } else if (deltaVal < 0) {
        el.textContent = `${deltaVal} مقارنةً بالتشغيل السابق (${prevDate})`;
        el.classList.add("delta-negative");
      } else {
        el.textContent = `لا تغيير مقارنةً بالتشغيل السابق (${prevDate})`;
        el.classList.add("delta-neutral");
      }
    }

    updateDelta(document.getElementById("card-not-visited-delta"), prevNot, data.delta_not_visited, "لم تزار");
    updateDelta(document.getElementById("card-visited-delta"), prevVisited, data.delta_visited, "تمت الزيارة");
    updateDelta(document.getElementById("card-total-delta"), prevTotal, data.delta_total, "اجمالي الزيارات");

    if (statusDiv) statusDiv.textContent = "";

  } catch (e) {
    console.error(e);
    if (statusDiv) statusDiv.textContent = "خطأ في الاتصال بالخادم.";
  }
}

// ==================== بناء الجداول ====================
function buildTableHtml(rows, preferredOrder, title) {
  if (!rows || rows.length === 0) {
    return `<div class="table-empty">لا توجد بيانات لعرضها.</div>`;
  }
  const first = rows[0];
  let cols = [];
  if (preferredOrder && preferredOrder.length > 0) {
    cols = preferredOrder.filter((c) => c in first);
  }
  if (cols.length === 0) {
    cols = Object.keys(first);
  }

  let html = "";
  if (title) {
    html += `<div class="table-title">${title}</div>`;
  }
  html += `<div class="table-wrapper"><table class="data-table"><thead><tr>`;
  cols.forEach((c) => html += `<th>${c}</th>`);
  html += `</tr></thead><tbody>`;

  rows.forEach((row) => {
    html += "<tr>";
    cols.forEach((c) => {
      const val = row[c];
      html += `<td>${val === null || val === undefined ? "" : val}</td>`;
    });
    html += "</tr>";
  });
  html += "</tbody></table></div>";
  return html;
}

function setupSearchForContainer(container) {
  const input = container.querySelector(".table-search");
  const table = container.querySelector("table");
  if (!input || !table) return;
  const tbody = table.tBodies[0];
  if (!tbody) return;
  const rows = Array.from(tbody.rows);

  input.addEventListener("input", () => {
    const q = input.value.trim().toLowerCase();
    if (!q) {
      rows.forEach((row) => row.style.display = "");
      return;
    }
    rows.forEach((row) => {
      const text = row.textContent.toLowerCase();
      row.style.display = text.includes(q) ? "" : "none";
    });
  });
}

// ==================== تحميل تفاصيل البلدية ====================
async function loadMunicipalityDetails() {
  const summaryContainer = document.getElementById("muni-summary-table");
  const rawContainer = document.getElementById("muni-raw-table");
  if (!summaryContainer || !rawContainer) return;

  const bodySector = getBodySector();
  if (bodySector) {
    summaryContainer.innerHTML = "";
    rawContainer.innerHTML = "";
    return;
  }

  const muni = getMunicipalityFilter();
  if (!muni) {
    summaryContainer.innerHTML = "<div class='table-empty'>اختر بلديه لعرض التفاصيل</div>";
    rawContainer.innerHTML = "";
    return;
  }

  summaryContainer.innerHTML = "<div class='table-loading'>جاري تحميل تفاصيل البلدية...</div>";
  rawContainer.innerHTML = "";

  try {
    const resp = await fetch(`/api/municipality/${encodeURIComponent(muni)}/details`, { headers: getAuthHeader() });
    if (!resp.ok) {
      const err = await resp.json().catch(() => ({}));
      const msg = err.error || "تعذر تحميل تفاصيل البلدية.";
      summaryContainer.innerHTML = `<div class='table-error'>${msg}</div>`;
      return;
    }

    const data = await resp.json();
    let summaryRows = data.summary || [];
    summaryRows = summaryRows.map((row) => {
      const total = Number(row["إجمالي_الرخص"] ?? 0);
      const visited = Number(row["تمت الزيارة"] ?? 0);
      const pct = total > 0 ? (visited / total) * 100 : 0;
      return { ...row, "النسبة": `${pct.toFixed(1)}%` };
    });

    const rawRows = data.raw || [];
    const summaryOrder = ["التصنيف", "إجمالي_الرخص", "تمت الزيارة", "لم تزار", "النسبة"];
    const rawOrder = [
      "رقم الزيارة", "الامانة", "البلدية", "اسم الحي", "اسم المراقب", "تاريخ ووقت الاسناد",
      "تاريخ الاسناد", "وقت  اسناد الزيارة", "تاريخ بدء الزيارة", "تاريخ انهاء الزيارة", "مدة الزيارة",
      "نوع الرقابة", "درجة خطورة النشاط", "درجة الامتثال", "رقم البلاغ", "حالة الزيارة", "نوع الزيارة",
      "رقم الجهة", "اسم الجهة", "قيمة المخالفة", "هل المخالفة انذار", "رقم بند اللائحة", "رقم الرخصة",
      "اسم المنشأة", "المرحلة", "عدد البنود الغير ممتثلة", "اسم الادارة", "license_id_str", "الحالات",
      "MUNICIPALITY_EN", "التصنيف"
    ];

    const summaryTableHtml = buildTableHtml(summaryRows, summaryOrder, null);
    summaryContainer.innerHTML = `
      <div class="table-title">ملخص الزيارات حسب التصنيف</div>
      <div class="search-container">
        <input type="text" class="table-search search-input" placeholder="بحث..." />
        <span class="search-icon">🔍</span>
      </div>
      ${summaryTableHtml}
    `;

    const rawTableHtml = buildTableHtml(rawRows, rawOrder, null);
    rawContainer.innerHTML = `
      <div class="table-title">الزيارات وحالاتها </div>
      <div class="search-container">
        <input type="text" class="table-search search-input" placeholder="بحث..." />
      </div>
      ${rawTableHtml}
    `;

    setupSearchForContainer(summaryContainer);
    setupSearchForContainer(rawContainer);
  } catch (e) {
    console.error(e);
    summaryContainer.innerHTML = "<div class='table-error'>خطأ في الاتصال.</div>";
  }
}

// ==================== الرسم البياني ====================
async function loadChart() {
  const canvas = document.getElementById("visitsChart");
  if (!canvas) return;

  let query = "?scope=all";
  const bodySector = getBodySector();
  const muni = getMunicipalityFilter();
  const sector = getSectorKey();

  if (bodySector) query = `?scope=sector&sector=${encodeURIComponent(bodySector)}`;
  else if (muni) query = `?scope=municipality&municipality=${encodeURIComponent(muni)}`;
  else if (sector) query = `?scope=sector&sector=${encodeURIComponent(sector)}`;

  try {
    const resp = await fetch(`/api/chart-data/compare${query}`, { headers: getAuthHeader() });
    if (!resp.ok) return;

    const data = await resp.json();
    const ctx = canvas.getContext("2d");
    if (visitsChart) visitsChart.destroy();

    const datasets = [
      {
        label: "اليوم الحالي - تمت الزيارة",
        data: data.current_visited,
        backgroundColor: "#D4AF91",
        stack: "current",
      },
      {
        label: "اليوم الحالي - لم تزار",
        data: data.current_not,
        backgroundColor: "#973D4B",
        stack: "current",
      },
    ];

    if (data.has_prev) {
      datasets.push(
        {
          label: "اليوم السابق - تمت الزيارة",
          data: data.prev_visited,
          backgroundColor: "#E3A778",
          stack: "previous",
        },
        {
          label: "اليوم السابق - لم تزار",
          data: data.prev_not,
          backgroundColor: "#973D4B",
          stack: "previous",
        }
      );
    }

    visitsChart = new Chart(ctx, {
      type: "bar",
      data: { labels: data.labels, datasets: datasets },
      options: {
        responsive: true,
        plugins: {
          legend: { position: "top", labels: { font: { family: "Tajawal" } } },
        },
        scales: {
          x: { stacked: true, ticks: { font: { family: "Tajawal" } } },
          y: { stacked: true, beginAtZero: true, ticks: { font: { family: "Tajawal" } } },
        },
      },
    });
  } catch (e) { console.error(e); }
}

// ==================== رفع الملفات ====================
function setupUploadForm() {
  const form = document.getElementById("upload-form");
  if (!form) return;
  const statusDiv = document.getElementById("upload-status");

  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    if (statusDiv) statusDiv.textContent = "جاري رفع الملفات ...";
    const formData = new FormData(form);

    try {
      const resp = await fetch("/api/process", {
        method: "POST",
        body: formData,
        headers: getAuthHeader() // Note: fetch usually handles Multipart type, but we need Authorization
        // Note: do not set Content-Type manually for FormData
      });

      if (!resp.ok) {
        const err = await resp.json().catch(() => ({}));
        if (statusDiv) statusDiv.textContent = "خطأ: " + (err.error || resp.statusText);
        return;
      }
      await resp.json();
      if (statusDiv) statusDiv.textContent = "تمت المعالجة بنجاح.";
    } catch (err) {
      console.error(err);
      if (statusDiv) statusDiv.textContent = "خطأ في الاتصال بالخادم.";
    }
  });
}

document.addEventListener("DOMContentLoaded", () => {
  setupMenu();
  setupUploadForm();
  setupHomeFilters();
  loadTotals();
  loadChart();
  loadMunicipalityDetails();
});
