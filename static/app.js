const API = '';

let userRole = null;
let clientId = null;
let sseSource = null;
let subscribedCategories = new Set();
let knownCategories = new Set();

function setRole(role) {
  userRole = role;
  document.getElementById('role-select-screen').style.display = 'none';
  document.getElementById('main-content').style.display = 'grid';

  document.getElementById('btn-loja').classList.toggle('active', role === 'loja');
  document.getElementById('btn-consumidor').classList.toggle('active', role === 'consumidor');

  document.getElementById('section-cadastro').style.display   = role === 'loja'       ? '' : 'none';
  document.getElementById('section-interesses').style.display = role === 'consumidor' ? '' : 'none';
  document.getElementById('sse-status').style.display         = role === 'consumidor' ? '' : 'none';

  if (role === 'consumidor') {
    connectSSE();
  } else {
    if (sseSource) { sseSource.close(); sseSource = null; }
    clientId = null;
  }

  loadPromotions();
}

function setStatus(connected) {
  const el = document.getElementById('sse-status');
  el.textContent = connected ? 'conectado' : 'desconectado';
  el.className = 'status ' + (connected ? 'connected' : 'disconnected');
}

function connectSSE() {
  if (sseSource) return;
  sseSource = new EventSource(API + '/sse');

  sseSource.addEventListener('connected', e => {
    clientId = JSON.parse(e.data).client_id;
    setStatus(true);
    renderCategories();
    document.getElementById('section-notificacoes').style.display = '';
  });

  sseSource.addEventListener('promocao.categoria', e => {
    const data = JSON.parse(e.data);
    showToast(data, 'categoria');
    logNotification(data, 'categoria');
  });

  sseSource.addEventListener('promocao.destaque', e => {
    const data = JSON.parse(e.data);
    showToast(data, 'destaque');
    logNotification(data, 'destaque');
  });

  sseSource.onerror = () => {
    setStatus(false);
    clientId = null;
    renderCategories();
  };
}

function logNotification(data, type) {
  const log = document.getElementById('notif-log');
  const empty = document.getElementById('notif-empty');
  if (empty) empty.remove();

  const time = new Date().toLocaleTimeString('pt-BR');
  const row = document.createElement('div');
  row.className = 'notif-row';
  row.innerHTML =
    '<span class="notif-row-time">' + time + '</span>' +
    '<span class="notif-row-type ' + type + '">' + (type === 'destaque' ? 'hot deal' : 'categoria') + '</span>' +
    '<span class="notif-row-desc">' + (data.description || '') + '</span>';
  log.prepend(row);
}

function clearNotifications() {
  const log = document.getElementById('notif-log');
  log.innerHTML = '<p class="empty" id="notif-empty">nenhuma notificacao recebida.</p>';
}

function showToast(data, type) {
  const container = document.getElementById('toast-container');
  const el = document.createElement('div');
  el.className = 'toast ' + type;
  el.innerHTML =
    '<div class="toast-type">' + (type === 'destaque' ? 'hot deal' : data.category) + '</div>' +
    '<div class="toast-desc">' + (data.description || '') + '</div>';
  container.appendChild(el);
  requestAnimationFrame(() => el.classList.add('show'));
  setTimeout(() => {
    el.classList.remove('show');
    el.addEventListener('transitionend', () => el.remove(), { once: true });
  }, 5000);
}

async function loadPromotions() {
  const res = await fetch(API + '/promotions');
  const promos = await res.json();
  const container = document.getElementById('promotions-list');

  if (!promos || promos.length === 0) {
    container.innerHTML = '<p class="empty">nenhuma promocao.</p>';
    return;
  }

  promos.forEach(p => { if (p.category) knownCategories.add(p.category); });
  if (userRole === 'consumidor') renderCategories();

  container.innerHTML = '';
  promos.forEach(p => {
    const el = document.createElement('div');
    el.className = 'promo-row';
    const votes = userRole === 'consumidor'
      ? '<div class="promo-votes">' +
          '<button onclick="vote(\'' + p.id + '\', 1, this)">+1</button>' +
          '<button onclick="vote(\'' + p.id + '\', -1, this)">-1</button>' +
        '</div>'
      : '';
    el.innerHTML =
      '<span class="promo-cat">' + p.category + '</span>' +
      '<span class="promo-desc">' + p.description + '</span>' +
      votes;
    container.appendChild(el);
  });
}

async function vote(id, value, btn) {
  const res = await fetch(API + '/promotions/' + id + '/vote?intension=' + value, { method: 'POST' });
  if (res.ok) {
    btn.classList.add('voted');
    setTimeout(() => btn.classList.remove('voted'), 600);
  }
}

function renderCategories() {
  const container = document.getElementById('categories-tags');
  const hint = document.getElementById('categories-hint');
  if (!clientId) {
    container.innerHTML = '';
    hint.style.display = 'inline';
    return;
  }
  hint.style.display = 'none';
  if (knownCategories.size === 0) {
    container.innerHTML = '<span class="empty">nenhuma categoria disponivel.</span>';
    return;
  }
  container.innerHTML = '';
  knownCategories.forEach(cat => {
    const active = subscribedCategories.has(cat);
    const tag = document.createElement('span');
    tag.className = 'tag' + (active ? ' tag-active' : '');
    tag.textContent = active ? cat + ' x' : '+ ' + cat;
    tag.onclick = () => active ? unsubscribe(cat) : subscribe(cat);
    container.appendChild(tag);
  });
}

async function subscribe(category) {
  if (!clientId) return;
  const res = await fetch(API + '/promotions/subscribe?client_id=' + clientId + '&category=' + category, { method: 'POST' });
  if (res.ok) { subscribedCategories.add(category); renderCategories(); }
}

async function unsubscribe(category) {
  if (!clientId) return;
  const res = await fetch(API + '/promotions/unsubscribe?client_id=' + clientId + '&category=' + category, { method: 'POST' });
  if (res.ok) { subscribedCategories.delete(category); renderCategories(); }
}

document.getElementById('promo-form').addEventListener('submit', async e => {
  e.preventDefault();
  const store_email = document.getElementById('new-email').value.trim();
  const category    = document.getElementById('new-category').value.trim();
  const description = document.getElementById('new-description').value.trim();
  const feedback    = document.getElementById('promo-feedback');

  const res = await fetch(API + '/promotions', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ category, description, store_email })
  });

  if (res.ok) {
    feedback.textContent = 'cadastrada.';
    feedback.className = 'feedback success';
    document.getElementById('new-email').value = '';
    document.getElementById('new-category').value = '';
    document.getElementById('new-description').value = '';
  } else {
    feedback.textContent = 'erro.';
    feedback.className = 'feedback error';
  }
  setTimeout(() => { feedback.textContent = ''; }, 2500);
});
