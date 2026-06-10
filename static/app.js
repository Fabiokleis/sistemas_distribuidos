const API = '';

let userRole = null;
let clientId = null;
let sseSource = null;
let subscribedCategories = new Set();
let knownCategories = new Set();

function start() {
    connectSSE();
    renderCategories();
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
	console.log(data);
	showToast(data, 'categoria');
	logNotification(data, 'categoria');
    });

    sseSource.addEventListener('promocao.destaque', e => {
	const data = JSON.parse(e.data);
	console.log(data);
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
    renderCategories();

    container.innerHTML = '';
    promos.forEach(p => {
	const el = document.createElement('div');
	el.className = 'promo-row';
	const votes =
	      '<div class="promo-votes">' +
              '<button onclick="vote(\'' + p.id + '\', 1, this)">+1</button>' +
              '<button onclick="vote(\'' + p.id + '\', -1, this)">-1</button>' +
              '</div>'
	el.innerHTML =
	    '<span class="promo-cat">' + p.category + '</span>' +
	    '<span class="promo-desc">' + p.description + '</span>' +
	    votes;
	container.appendChild(el);
    });
}

loadPromotions()

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


start();
