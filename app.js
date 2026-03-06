(function () {
  'use strict';

  const PAGE_SIZE = 24;
  const DATA_URLS = {
    apps: 'deta/ipa_store_dataset.json',
    profiles: 'deta/mobileconfig_dataset.json'
  };

  let appsData = [];
  let profilesData = [];
  let appsFiltered = [];
  let profilesFiltered = [];
  let appsPage = 0;
  let searchQuery = '';

  const $ = (sel, root = document) => root.querySelector(sel);
  const $$ = (sel, root = document) => [...root.querySelectorAll(sel)];

  const searchEl = $('#search');
  const clearSearchBtn = $('#clear-search');
  const tabs = $$('.tab');
  const panelApps = $('#panel-apps');
  const panelProfiles = $('#panel-profiles');
  const appsList = $('#apps-list');
  const profilesList = $('#profiles-list');
  const appsLoading = $('#apps-loading');
  const appsError = $('#apps-error');
  const appsEmpty = $('#apps-empty');
  const appsMoreWrap = $('#apps-more');
  const loadMoreBtn = $('#load-more-apps');
  const profilesLoading = $('#profiles-loading');
  const profilesError = $('#profiles-error');
  const profilesEmpty = $('#profiles-empty');

  function show(el, visible) {
    if (!el) return;
    el.hidden = !visible;
  }

  function escapeHtml(s) {
    const div = document.createElement('div');
    div.textContent = s;
    return div.innerHTML;
  }

  function formatBytes(n) {
    if (n == null || n === '') return '';
    const num = parseInt(n, 10);
    if (isNaN(num)) return n;
    if (num < 1024) return num + ' B';
    if (num < 1024 * 1024) return (num / 1024).toFixed(1) + ' KB';
    return (num / (1024 * 1024)).toFixed(1) + ' MB';
  }

  function formatDate(iso) {
    if (!iso) return '';
    try {
      const d = new Date(iso);
      return d.toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' });
    } catch (_) {
      return iso;
    }
  }

  function filterApps(query) {
    const q = (query || '').trim().toLowerCase();
    if (!q) return appsData;
    return appsData.filter(function (app) {
      const name = (app.app_name || '').toLowerCase();
      const dev = (app.developer || '').toLowerCase();
      const repo = (app.repository_name || '').toLowerCase();
      const desc = (app.description || '').toLowerCase();
      return name.includes(q) || dev.includes(q) || repo.includes(q) || desc.includes(q);
    });
  }

  function filterProfiles(query) {
    const q = (query || '').trim().toLowerCase();
    if (!q) return profilesData;
    return profilesData.filter(function (p) {
      const name = (p.name || '').toLowerCase();
      const desc = (p.description || '').toLowerCase();
      const org = (p.organization || '').toLowerCase();
      return name.includes(q) || desc.includes(q) || org.includes(q);
    });
  }

  function renderAppCard(app) {
    const name = escapeHtml(app.app_name || 'Unnamed');
    const dev = escapeHtml(app.developer || '');
    const repo = escapeHtml(app.repository_name || '');
    const desc = escapeHtml((app.description || '').slice(0, 140));
    const version = escapeHtml(app.version || '');
    const size = formatBytes(app.file_size);
    const date = formatDate(app.release_date);
    const url = app.ipa_download_url || '#';
    const repoUrl = app.repository_url || '#';

    const meta = [dev, repo, version].filter(Boolean).join(' · ');
    const extra = [size, date].filter(Boolean).join(' · ');

    const li = document.createElement('li');
    li.innerHTML =
      '<a class="card" href="' + escapeHtml(url) + '" target="_blank" rel="noopener">' +
        '<h2 class="card-title">' + name + '</h2>' +
        '<p class="card-meta">' + escapeHtml(meta) + '</p>' +
        (extra ? '<p class="card-meta">' + escapeHtml(extra) + '</p>' : '') +
        (desc ? '<p class="card-desc">' + desc + '</p>' : '') +
        '<div class="card-actions">' +
          '<span class="btn btn-primary">Install IPA</span>' +
          (repoUrl !== '#' ? '<a class="btn btn-secondary" href="' + escapeHtml(repoUrl) + '" target="_blank" rel="noopener" onclick="event.preventDefault();event.stopPropagation();window.open(this.href)">Repo</a>' : '') +
        '</div>' +
      '</a>';
    return li;
  }

  function renderProfileCard(profile) {
    const name = escapeHtml(profile.name || 'Unnamed profile');
    const desc = escapeHtml((profile.description || '').slice(0, 160));
    const url = profile.download_url || ('deta/mobileconfigs/' + (profile.uuid || '') + '.mobileconfig');

    const li = document.createElement('li');
    li.innerHTML =
      '<a class="card" href="' + escapeHtml(url) + '" download>' +
        '<h2 class="card-title">' + name + '</h2>' +
        (profile.organization ? '<p class="card-meta">' + escapeHtml(profile.organization) + '</p>' : '') +
        (desc ? '<p class="card-desc">' + desc + '</p>' : '') +
        '<div class="card-actions">' +
          '<span class="btn btn-primary">Install profile</span>' +
        '</div>' +
      '</a>';
    return li;
  }

  function renderAppsList(append) {
    const start = append ? appsPage * PAGE_SIZE : 0;
    const end = start + PAGE_SIZE;
    const slice = appsFiltered.slice(start, end);

    if (!append) appsList.innerHTML = '';

    slice.forEach(function (app) {
      appsList.appendChild(renderAppCard(app));
    });

    const total = appsFiltered.length;
    const shown = Math.min(end, total);
    show(appsEmpty, total === 0);
    show(appsMoreWrap, total > PAGE_SIZE && shown < total);
    if (loadMoreBtn) loadMoreBtn.textContent = 'Load more (' + shown + ' / ' + total + ')';
  }

  function renderProfilesList() {
    profilesList.innerHTML = '';
    profilesFiltered.forEach(function (p) {
      profilesList.appendChild(renderProfileCard(p));
    });
    show(profilesEmpty, profilesFiltered.length === 0);
  }

  function onSearch() {
    searchQuery = (searchEl && searchEl.value) || '';
    if (clearSearchBtn) clearSearchBtn.hidden = !searchQuery.trim();

    appsFiltered = filterApps(searchQuery);
    profilesFiltered = filterProfiles(searchQuery);
    appsPage = 0;

    renderAppsList(false);
    renderProfilesList();
  }

  function loadApps() {
    show(appsLoading, true);
    show(appsError, false);
    fetch(DATA_URLS.apps)
      .then(function (r) {
        if (!r.ok) throw new Error(r.status + ' ' + r.statusText);
        return r.json();
      })
      .then(function (data) {
        appsData = data.apps || [];
        appsFiltered = filterApps(searchQuery);
        appsPage = 0;
        show(appsLoading, false);
        renderAppsList(false);
      })
      .catch(function (err) {
        show(appsLoading, false);
        if (appsError) {
          appsError.textContent = 'Could not load apps. ' + (err.message || '');
          show(appsError, true);
        }
      });
  }

  function loadProfiles() {
    show(profilesLoading, true);
    show(profilesError, false);
    fetch(DATA_URLS.profiles)
      .then(function (r) {
        if (!r.ok) throw new Error(r.status + ' ' + r.statusText);
        return r.json();
      })
      .then(function (data) {
        profilesData = data.profiles || [];
        profilesFiltered = filterProfiles(searchQuery);
        show(profilesLoading, false);
        renderProfilesList();
      })
      .catch(function (err) {
        show(profilesLoading, false);
        if (profilesError) {
          profilesError.textContent = 'Could not load profiles. ' + (err.message || '');
          show(profilesError, true);
        }
      });
  }

  function setTab(activeTab) {
    tabs.forEach(function (t) {
      const isActive = t.getAttribute('data-tab') === activeTab;
      t.classList.toggle('active', isActive);
      t.setAttribute('aria-selected', isActive);
    });
    const appsActive = activeTab === 'apps';
    show(panelApps, true);
    panelApps.classList.toggle('active', appsActive);
    show(panelProfiles, true);
    panelProfiles.classList.toggle('active', !appsActive);
    panelProfiles.hidden = appsActive;
  }

  if (searchEl) {
    searchEl.addEventListener('input', onSearch);
    searchEl.addEventListener('keydown', function (e) {
      if (e.key === 'Escape') {
        searchEl.value = '';
        onSearch();
        searchEl.blur();
      }
    });
  }

  if (clearSearchBtn) {
    clearSearchBtn.addEventListener('click', function () {
      if (searchEl) searchEl.value = '';
      onSearch();
      searchEl.focus();
    });
  }

  tabs.forEach(function (t) {
    t.addEventListener('click', function () {
      setTab(t.getAttribute('data-tab'));
    });
  });

  if (loadMoreBtn) {
    loadMoreBtn.addEventListener('click', function () {
      appsPage++;
      renderAppsList(true);
    });
  }

  setTab('apps');
  loadApps();
  loadProfiles();
})();
