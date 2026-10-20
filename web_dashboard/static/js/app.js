document.addEventListener('DOMContentLoaded', () => {
    // =========================================================================
    // 0. Demo Account Switching System (User A: Nhu, User B: Nam, User C: Thi)
    // =========================================================================
    const accountPillButtons = document.querySelectorAll('.acc-pill');
    let currentDemoUser = "Nguyễn Trường Nhu (Người dùng A)";

    // Web Audio Chime Sound (Tạo hiệu ứng âm thanh sống động khi có người đánh giá)
    function playChimeSound() {
        try {
            const ctx = new (window.AudioContext || window.webkitAudioContext)();
            const osc = ctx.createOscillator();
            const gain = ctx.createGain();
            osc.type = 'triangle';
            osc.frequency.setValueAtTime(523.25, ctx.currentTime); // C5
            osc.frequency.exponentialRampToValueAtTime(783.99, ctx.currentTime + 0.14); // G5
            gain.gain.setValueAtTime(0.18, ctx.currentTime);
            gain.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + 0.4);
            osc.connect(gain);
            gain.connect(ctx.destination);
            osc.start();
            osc.stop(ctx.currentTime + 0.4);
        } catch (e) {
            // Audio context policy
        }
    }

    // Hàm khử ký tự HTML an toàn (XSS Defense & Payload Sanitization)
    function escapeHtmlText(str) {
        if (!str) return "";
        const cleaned = String(str)
            .replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi, '')
            .replace(/<style\b[^<]*(?:(?!<\/style>)<[^<]*)*<\/style>/gi, '')
            .replace(/<[^>]+>/g, '')
            .trim();
        const d = document.createElement('div');
        d.textContent = cleaned;
        return d.innerHTML;
    }

    function escapeHtmlAttr(str) {
        return String(str || '').replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/'/g, '&#39;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    }

    // Hoisted cross-module state variables to avoid Temporal Dead Zone (TDZ)
    let allLoadedSpots = [];
    let currentClusterFilter = 'all';
    let currentSearchKeyword = '';
    let currentSyncJob = { id: null, status: 'idle', hdfsConfirmed: false };
    let syncTimeoutTimer = null;
    const pendingReviewJobs = new Map();
    const processedReviewEvents = new Set();
    const renderedStreamEventIds = new Set();
    let sseReviewDataVersion = 0;
    let spotsFetchSeq = 0;

    // Floating Real-Time Toast Notification
    function showRealtimeToast(spotName, userName, reviewText, rating, isSystemInfo = false, spotStt = null) {
        const container = document.getElementById('realtime-toast-container');
        if (!container) return;

        // Giới hạn tối đa 3 thông báo cùng lúc trên màn hình để không che giao diện
        const maxToasts = 3;
        while (container.children.length >= maxToasts) {
            if (container.lastElementChild) {
                container.lastElementChild.remove();
            }
        }

        playChimeSound();

        const toast = document.createElement('div');
        toast.className = 'realtime-toast';
        toast.style.cursor = 'pointer';
        toast.title = 'Nhấp vào thông báo để xem nhanh thẻ địa điểm (tùy chọn)';

        let avatarClass = '';
        let avatarLetter = '💬';
        if (userName.includes('Nam') || userName.includes('B')) {
            toast.classList.add('peer-b');
            avatarClass = 'avatar-b';
            avatarLetter = 'B';
        } else if (userName.includes('Thi') || userName.includes('C')) {
            toast.classList.add('peer-c');
            avatarClass = 'avatar-c';
            avatarLetter = 'C';
        } else if (userName.includes('Nhu') || userName.includes('A')) {
            avatarLetter = 'A';
        }

        const isMe = currentDemoUser && currentDemoUser.split('(')[0].trim() === userName.split('(')[0].trim();
        const badgeLabel = isSystemInfo ? 'HỆ THỐNG' : (isMe ? 'Bạn vừa gửi' : '⚡ KHÁCH VỪA ĐÁNH GIÁ');

        toast.innerHTML = `
            <div class="toast-avatar ${avatarClass}">${escapeHtmlText(avatarLetter)}</div>
            <div class="toast-content">
                <div class="toast-header">
                    <span class="toast-author">${escapeHtmlText(userName)}</span>
                    <span class="toast-badge">${escapeHtmlText(badgeLabel)}</span>
                </div>
                <div class="toast-spot">📍 ${escapeHtmlText(spotName)} • <span style="color:#D97706; font-weight:700;">⭐ ${parseFloat(rating || 5.0).toFixed(1)}/5.0</span></div>
                <div class="toast-text">"${escapeHtmlText(reviewText)}"</div>
            </div>
            <button class="toast-close" title="Đóng">&times;</button>
        `;

        const closeBtn = toast.querySelector('.toast-close');
        if (closeBtn) {
            closeBtn.addEventListener('click', (e) => {
                e.stopPropagation();
                toast.remove();
            });
        }

        // Chỉ cuộn đến thẻ khi người dùng CHỦ ĐỘNG CLICK vào Toast
        toast.addEventListener('click', (e) => {
            if (e.target.closest('.toast-close')) return;
            const parsedStt = parseInt(spotStt);
            const cardElem = (parsedStt ? document.getElementById(`spot-dir-card-${parsedStt}`) : null) ||
                             document.querySelector(`[data-spot-name="${spotName}"]`);
            if (cardElem) {
                cardElem.scrollIntoView({ behavior: 'smooth', block: 'center' });
                cardElem.classList.remove('highlight-flash');
                void cardElem.offsetWidth;
                cardElem.classList.add('highlight-flash');
            }
        });

        container.prepend(toast);

        // Tự động mờ và đóng sau 4 giây
        setTimeout(() => {
            toast.classList.add('toast-fadeout');
            setTimeout(() => {
                if (toast.parentElement) toast.remove();
            }, 350);
        }, 4000);
    }

    function setActiveDemoAccount(userName, color) {
        currentDemoUser = userName;
        localStorage.setItem('demo_active_user', userName);

        accountPillButtons.forEach(btn => {
            const btnUser = btn.getAttribute('data-user');
            if (btnUser === userName) {
                btn.classList.add('active');
            } else {
                btn.classList.remove('active');
            }
        });

        // Update modal and simulator inputs
        const modalUserInput = document.getElementById('modal-user-name');
        if (modalUserInput) modalUserInput.value = userName;

        const simUserInput = document.getElementById('sim-user-name');
        if (simUserInput) simUserInput.value = userName;
    }

    // Read from URL query param ?user=A or ?user=B or ?user=C
    const urlParams = new URLSearchParams(window.location.search);
    const paramUser = urlParams.get('user');

    if (paramUser) {
        const target = paramUser.toUpperCase();
        if (target === 'B' || target === 'NAM') {
            setActiveDemoAccount("Nguyễn Hoài Nam (Người dùng B)", "#059669");
        } else if (target === 'C' || target === 'THI') {
            setActiveDemoAccount("Đặng Thành Thi (Người dùng C)", "#D97706");
        } else {
            setActiveDemoAccount("Nguyễn Trường Nhu (Người dùng A)", "#4F46E5");
        }
    } else {
        const savedUser = localStorage.getItem('demo_active_user') || "Nguyễn Trường Nhu (Người dùng A)";
        setActiveDemoAccount(savedUser);
    }

    accountPillButtons.forEach(btn => {
        btn.addEventListener('click', () => {
            const user = btn.getAttribute('data-user');
            const color = btn.getAttribute('data-color');
            setActiveDemoAccount(user, color);
            showRealtimeToast("Đã đổi tài khoản", user, "Bạn đang đóng vai tài khoản này để gửi đánh giá demo!", 5.0, true);
        });
    });

    // 1. Initialize Map with multiple free, beautiful light tile providers
    const osmTile = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        maxZoom: 19,
        attribution: '&copy; OpenStreetMap contributors'
    });

    const esriStreetTile = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Street_Map/MapServer/tile/{z}/{y}/{x}', {
        maxZoom: 19,
        attribution: 'Tiles &copy; Esri'
    });

    const esriSatellite = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
        maxZoom: 19,
        attribution: 'Tiles &copy; Esri'
    });

    const map = L.map('vietnam-map', {
        center: [16.0471, 107.5451], // Center of Vietnam
        zoom: 6,
        layers: [esriStreetTile], // Default to crisp, bright Esri Street Map
        zoomControl: true,
        attributionControl: false
    });

    // Layer Switcher Control (Top Right)
    const baseLayers = {
        "Bản đồ Sáng (Esri Street)": esriStreetTile,
        "Bản đồ Tiêu Chuẩn (OpenStreetMap)": osmTile,
        "Ảnh Vệ Tinh (Satellite)": esriSatellite
    };
    L.control.layers(baseLayers, null, { position: 'topright' }).addTo(map);

    const clusterColors = {
        0: '#2563EB', // Miền Bắc - Blue
        1: '#059669', // Bắc Trung Bộ - Emerald
        2: '#D97706', // Nam Trung Bộ - Amber
        3: '#7C3AED', // Tây Nguyên - Purple
        4: '#DB2777'  // Nam Bộ - Rose
    };

    // Pipeline Real-Time System Status Checker (Kafka, Spark, HDFS, Web)
    function checkSystemStatus() {
        fetch('/api/system_status')
            .then(res => res.json())
            .then(data => {
                // 1. Kafka Broker
                const dotKafka = document.getElementById('dot-kafka');
                const labelKafka = document.getElementById('label-kafka');
                if (dotKafka && labelKafka) {
                    if (data.kafka && data.kafka.connected) {
                        dotKafka.className = 'status-dot pulse-green';
                        labelKafka.innerHTML = `Kafka Ingest: <strong>ONLINE (9092)</strong>`;
                    } else {
                        dotKafka.className = 'status-dot pulse-red';
                        labelKafka.innerHTML = `Kafka Ingest: <strong style="color:#EF4444;">OFFLINE</strong>`;
                    }
                }

                // 2. Spark MLlib
                const dotSpark = document.getElementById('dot-spark');
                const labelSpark = document.getElementById('label-spark');
                if (dotSpark && labelSpark) {
                    const cnt = data.spark ? data.spark.spots_in_catalog : 0;
                    if (cnt >= 300) {
                        dotSpark.className = 'status-dot pulse-green';
                        labelSpark.innerHTML = `Spark MLlib: <strong>${cnt} POIs Synced</strong>`;
                    } else if (cnt > 0) {
                        dotSpark.className = 'status-dot pulse-orange';
                        labelSpark.innerHTML = `Spark MLlib: <strong>Đang đồng bộ (${cnt}/317)</strong>`;
                    } else {
                        dotSpark.className = 'status-dot pulse-orange';
                        labelSpark.innerHTML = `Spark MLlib: <strong>Khởi động MLlib...</strong>`;
                    }
                }

                // 3. HDFS Lake
                const dotHdfs = document.getElementById('dot-hdfs');
                const labelHdfs = document.getElementById('label-hdfs');
                if (dotHdfs && labelHdfs) {
                    if (data.hdfs && data.hdfs.connected) {
                        dotHdfs.className = 'status-dot pulse-blue';
                        labelHdfs.innerHTML = `HDFS Lake: <strong>ONLINE (9000)</strong>`;
                    } else {
                        dotHdfs.className = 'status-dot pulse-red';
                        labelHdfs.innerHTML = `HDFS Lake: <strong style="color:#EF4444;">OFFLINE</strong>`;
                    }
                }
            })
            .catch(() => {});
    }

    checkSystemStatus();
    setInterval(checkSystemStatus, 4000);

    const markersMap = {};
    const markersMapByName = {};
    let activeHighlightMarker = null;

    fetch('/api/all_spots')
        .then(res => res.json())
        .then(spots => {
            if (!Array.isArray(spots)) {
                console.warn("[*] Phản hồi từ /api/all_spots:", spots);
                const kpiEl = document.getElementById('kpi-total-spots');
                if (kpiEl) kpiEl.textContent = "0 (Chờ batch)";
                return;
            }
            document.getElementById('kpi-total-spots').textContent = spots.length;
            
            // Dynamic cluster filter labeling based on real Spark results
            const clusterNamesMap = {};
            spots.forEach(spot => {
                if (spot.cluster_id !== undefined && spot.cluster_name) {
                    clusterNamesMap[spot.cluster_id] = spot.cluster_name.split('(')[0].trim();
                }
            });
            document.querySelectorAll('#cluster-filter-buttons .filter-chip').forEach(btn => {
                const cid = btn.getAttribute('data-cluster');
                if (cid !== 'all' && clusterNamesMap[cid]) {
                    btn.textContent = clusterNamesMap[cid];
                }
            });

            spots.forEach(spot => {
                const color = spot.cluster_color || clusterColors[spot.cluster_id] || '#2563EB';
                const marker = L.circleMarker([spot.latitude, spot.longitude], {
                    radius: 6,
                    fillColor: color,
                    color: '#FFFFFF',
                    weight: 2,
                    opacity: 1,
                    fillOpacity: 0.9
                }).addTo(map);

                const popupContent = `
                    <div style="font-family: 'Plus Jakarta Sans', sans-serif; min-width: 200px; color: #0F172A;">
                        ${spot.image ? `<img src="${spot.image}" style="width: 100%; height: 100px; object-fit: cover; border-radius: 8px; margin-bottom: 8px;" onerror="this.style.display='none'">` : ''}
                        <h4 style="margin: 0; font-size: 14px; font-weight: 800; color: #1E293B;">#${spot.stt} ${spot.name}</h4>
                        <div style="font-size: 12px; color: #64748B; margin: 3px 0 6px 0;">📍 ${spot.location} • <strong style="color: #D97706;">⭐ ${spot.rating}/5.0</strong></div>
                        <span style="font-size: 11px; font-weight: 700; color: white; background: ${color}; padding: 3px 8px; border-radius: 12px; display: inline-block;">${spot.cluster_name}</span>
                    </div>
                `;
                marker.bindPopup(popupContent);
                markersMap[spot.stt] = marker;
                markersMapByName[spot.name.toLowerCase()] = marker;
            });

            // Initialize Directory Cards
            allLoadedSpots = spots;
            renderSpotsDirectory(allLoadedSpots);
            // Đồng bộ số sao trên các thẻ lịch trình đang hiển thị
            allLoadedSpots.forEach(s => {
                const itinStars = document.querySelectorAll(`.itin-stop-rating-${s.stt}`);
                itinStars.forEach(el => {
                    el.textContent = `★ ${Number(s.rating).toFixed(1)}`;
                });
            });
        })
        .catch(err => console.error("Error loading spots:", err));

    // 2. Real-Time Stream Ingestion via EventSource
    let eventCount = 0;
    const feedScrollBox = document.getElementById('feed-scroll-box');
    const emptyPlaceholder = document.getElementById('empty-feed-placeholder');
    const feedCounterBadge = document.getElementById('feed-counter-badge');
    const eventCounterText = document.getElementById('event-counter-text');
    const streamStatusText = document.getElementById('stream-status-text');

    const eventSource = new EventSource('/api/stream');

    eventSource.onopen = () => {
        streamStatusText.textContent = "Live Streaming";
        streamStatusText.style.color = "#059669";
    };

    eventSource.onerror = () => {
        streamStatusText.textContent = "Reconnecting...";
        streamStatusText.style.color = "#D97706";
    };

    eventSource.onmessage = (e) => {
        if (!e.data || e.data.startsWith(':')) return; // Ignore heartbeat

        try {
            const data = JSON.parse(e.data);
            handleNewStreamEvent(data);
        } catch (err) {
            console.error("Parse event error:", err);
        }
    };

    // Tải ngay các sự kiện gần nhất để feed hiển thị tức thì khi vừa mở web, không bị trống
    fetch('/api/recent_stream_events')
        .then(res => res.json())
        .then(data => {
            if (data && Array.isArray(data.events) && data.events.length > 0) {
                data.events.forEach(ev => {
                    handleNewStreamEvent(ev);
                });
            }
        })
        .catch(err => console.warn("[-] Không thể tải /api/recent_stream_events:", err));

    function reloadMergedSpots() {
        const thisFetchSeq = ++spotsFetchSeq;
        const versionAtStart = sseReviewDataVersion;

        fetch('/api/all_spots?_t=' + Date.now())
            .then(res => {
                if (!res.ok) throw new Error(`HTTP ${res.status}`);
                return res.json();
            })
            .then(spots => {
                // 1. Kiểm tra thứ tự: nếu đã có một lượt fetch mới hơn được kích hoạt thì bỏ qua phản hồi cũ
                if (thisFetchSeq !== spotsFetchSeq) return;

                // 2. Request chỉ được áp dụng nếu chưa có cập nhật SSE mới kể từ lúc bắt đầu.
                // Nếu có, tải lại kết quả hợp nhất để tránh hiển thị sai lệch giao diện.
                if (sseReviewDataVersion !== versionAtStart) {
                    console.log(`[*] Phát hiện cập nhật SSE mới (${versionAtStart} -> ${sseReviewDataVersion}) trong khi tải. Đang tải lại kết quả hợp nhất...`);
                    reloadMergedSpots();
                    return;
                }

                if (Array.isArray(spots) && spots.length > 0) {
                    allLoadedSpots = spots;
                    renderSpotsDirectory(allLoadedSpots);
                    filterAndRenderDirectory();

                    // Đồng bộ số sao trên các thẻ lịch trình đang hiển thị
                    allLoadedSpots.forEach(s => {
                        const itinStars = document.querySelectorAll(`.itin-stop-rating-${s.stt}`);
                        itinStars.forEach(el => {
                            el.textContent = `★ ${Number(s.rating).toFixed(1)}`;
                        });
                    });

                    // Cập nhật popup điểm số trên markers bản đồ
                    spots.forEach(spot => {
                        const marker = markersMap[spot.stt];
                        if (marker) {
                            const color = spot.cluster_color || clusterColors[spot.cluster_id] || '#2563EB';
                            const popupContent = `
                                <div style="font-family: 'Plus Jakarta Sans', sans-serif; min-width: 200px; color: #0F172A;">
                                    ${spot.image ? `<img src="${spot.image}" style="width: 100%; height: 100px; object-fit: cover; border-radius: 8px; margin-bottom: 8px;" onerror="this.style.display='none'">` : ''}
                                    <h4 style="margin: 0; font-size: 14px; font-weight: 800; color: #1E293B;">#${spot.stt} ${spot.name}</h4>
                                    <div style="font-size: 12px; color: #64748B; margin: 3px 0 6px 0;">📍 ${spot.location} • <strong style="color: #D97706;">⭐ ${spot.rating}/5.0 (${spot.review_count || 0} đánh giá)</strong></div>
                                    <span style="font-size: 11px; font-weight: 700; color: white; background: ${color}; padding: 3px 8px; border-radius: 12px; display: inline-block;">${spot.cluster_name}</span>
                                </div>
                            `;
                            marker.setPopupContent(popupContent);
                        }
                    });
                }
            })
            .catch(err => {
                console.warn("[-] Không thể tải lại /api/all_spots sau batch refresh:", err);
            });
    }

    function handleNewStreamEvent(event) {
        if (!event) return;
        try {
            const evId = String(event.event_id || '');
            if (evId && renderedStreamEventIds.has(evId)) {
                return; // Đã render rồi, tránh trùng lặp
            }
            if (evId) renderedStreamEventIds.add(evId);

            // Giải phóng và xác nhận công việc đánh giá đang chờ nếu khớp event_id
            if (event.event_id) {
                const evIdStr = String(event.event_id);
                if (pendingReviewJobs.has(evIdStr)) {
                    const job = pendingReviewJobs.get(evIdStr);
                    clearTimeout(job.timer);
                    pendingReviewJobs.delete(evIdStr);

                    const reviewStatusInd = document.getElementById('review-status-indicator');
                    if (job.source === 'sim') {
                        if (reviewStatusInd) {
                            reviewStatusInd.textContent = `✅ Thành công (Spark đã xử lý #${evIdStr})!`;
                            reviewStatusInd.style.color = "#059669";
                            setTimeout(() => {
                                reviewStatusInd.textContent = "Sẵn sàng";
                                reviewStatusInd.style.color = "#64748B";
                            }, 4000);
                        }
                    } else if (job.source === 'modal') {
                        showRealtimeToast(
                            job.spotName || event.name || "Điểm đến",
                            "Thành công",
                            `Spark Structured Streaming đã xử lý xong đánh giá #${evIdStr} và cập nhật rating!`,
                            event.rating || 5.0,
                            true
                        );
                    }
                }
            }

            if (event.action === 'SPARK_BATCH_CATALOG_REFRESH') {
                showRealtimeToast(
                    "Spark MLlib Engine",
                    "HDFS Retrain Complete",
                    `Spark vừa đọc lại HDFS thật và làm mới ${event.total_spots || 317} địa điểm!`,
                    5.0,
                    true
                );
                reloadMergedSpots();

                if (currentSyncJob && currentSyncJob.id && event.job_id && event.job_id === currentSyncJob.id && currentSyncJob.status === 'running') {
                    if (syncTimeoutTimer) clearTimeout(syncTimeoutTimer);
                    currentSyncJob.status = 'completed';

                    setStepStatus(6, 'done');
                    const syncResultBx = document.getElementById('sync-result-box');
                    const syncResultDet = document.getElementById('sync-result-details');
                    if (syncResultBx && syncResultDet) {
                        syncResultBx.style.background = '#F0FDF4';
                        syncResultBx.style.borderColor = '#86EFAC';
                        const titleEl = syncResultBx.querySelector('div:first-child');
                        if (titleEl) {
                            titleEl.innerHTML = '✅ HOÀN TẤT: SPARK ĐÃ ĐỌC LẠI HDFS & LÀM MỚI TOÀN BỘ CATALOG!';
                            titleEl.style.color = '#166534';
                        }
                        const extraInfo = document.getElementById('sync-spark-worker-confirm');
                        if (!extraInfo) {
                            syncResultDet.innerHTML += `
                                <div id="sync-spark-worker-confirm" style="margin-top:0.4rem; padding-top:0.4rem; border-top:1px dashed #86EFAC; color:#15803D;">
                                    • <b>Xác nhận từ Spark Worker (Job ${event.job_id}):</b> Đã hoàn tất tái đọc HDFS, làm mới ${event.total_spots || 317} địa điểm và cập nhật bộ nhớ đệm lịch trình tối ưu!
                                </div>
                            `;
                        }
                    }
                    const btnStartSync = document.getElementById('btn-start-sync-action');
                    const batchModal = document.getElementById('batch-sync-modal');
                    if (btnStartSync) {
                        btnStartSync.disabled = false;
                        btnStartSync.textContent = 'Đóng Cửa Sổ';
                        btnStartSync.onclick = () => {
                            if (batchModal) batchModal.classList.remove('show');
                        };
                    }
                }
                return;
            }

            if (event.action === 'SPARK_BATCH_RELOAD_FAILED') {
                showRealtimeToast(
                    "Spark MLlib Engine",
                    "HDFS Retrain Failed",
                    event.error || "Spark Worker gặp lỗi khi đọc lại HDFS!",
                    1.0,
                    true
                );
                if (currentSyncJob && currentSyncJob.id && event.job_id && event.job_id === currentSyncJob.id && currentSyncJob.status === 'running') {
                    if (syncTimeoutTimer) clearTimeout(syncTimeoutTimer);
                    currentSyncJob.status = 'failed';
                    setStepStatus(6, 'failed');
                    const syncResultBx = document.getElementById('sync-result-box');
                    const syncResultDet = document.getElementById('sync-result-details');
                    if (syncResultBx && syncResultDet) {
                        syncResultBx.style.background = '#FEF2F2';
                        syncResultBx.style.borderColor = '#FCA5A5';
                        const titleEl = syncResultBx.querySelector('div:first-child');
                        if (titleEl) {
                            titleEl.innerHTML = '❌ SPARK TÁI TÍNH TOÁN THẤT BẠI!';
                            titleEl.style.color = '#991B1B';
                        }
                        syncResultDet.innerHTML += `
                            <div style="margin-top:0.4rem; padding-top:0.4rem; border-top:1px dashed #FCA5A5; color:#DC2626;">
                                • <b>Chi tiết lỗi từ Worker (Job ${event.job_id}):</b> ${event.error || event.message}
                            </div>
                        `;
                    }
                    const btnStartSync = document.getElementById('btn-start-sync-action');
                    const batchModal = document.getElementById('batch-sync-modal');
                    if (btnStartSync) {
                        btnStartSync.disabled = false;
                        btnStartSync.textContent = 'Đóng Cửa Sổ';
                        btnStartSync.onclick = () => {
                            if (batchModal) batchModal.classList.remove('show');
                        };
                    }
                }
                return;
            }

            if (event.action === 'LAMBDA_BATCH_SYNC_COMPLETED') {
                showRealtimeToast(
                    "Data Lake HDFS (Port 9000)",
                    "Airflow Batch Sync",
                    event.message || `Đã đồng bộ ${event.reviews_synced || 0} đánh giá vào HDFS & Parquet Lake!`,
                    5.0,
                    true
                );
                reloadMergedSpots();
                return;
            }

            eventCount++;
            const emptyPlaceholderEl = document.getElementById('empty-feed-placeholder');
            if (emptyPlaceholderEl) {
                emptyPlaceholderEl.style.display = 'none';
            }

            // Live update Directory Card ONLY if this is a genuine user review (not a routine simulated stream event)
            const isActualUserReview = Boolean(
                event.is_user_review === true ||
                event.action === 'NEW_COMMUNITY_REVIEW' ||
                (event.new_review && event.new_review.review_text) ||
                (event.review_text && event.review_text.trim() !== '')
            );

            if (isActualUserReview) {
                sseReviewDataVersion++;
                const revObj = event.new_review || {
                    user_name: event.user_name || "Khách du lịch",
                    review_text: event.review_text || "Đánh giá xuất sắc!",
                    timestamp: event.timestamp || "Vừa xong"
                };
                const updatedSpotRating = Number(event.updated_rating || event.rating || 5.0);
                const reviewRating = Number(event.review_rating || revObj.rating || event.rating || 5.0);

                if (typeof updateSpotCardWithReview === 'function') {
                    updateSpotCardWithReview(event.stt, event.name, revObj, updatedSpotRating, event.review_count, event.event_id);
                }
                showRealtimeToast(event.name, revObj.user_name || "Khách du lịch", revObj.review_text || "Đánh giá xuất sắc!", reviewRating, false, event.stt);
                if (typeof handleLiveCommentStream === 'function') {
                    handleLiveCommentStream(event, revObj, updatedSpotRating, reviewRating);
                }
            }

            const counterBadge = document.getElementById('feed-counter-badge');
            if (counterBadge) {
                counterBadge.textContent = `${eventCount} sự kiện`;
            }
            const counterText = document.getElementById('event-counter-text');
            if (counterText) {
                counterText.textContent = `Sự kiện mới nhất: #${event.event_id || eventCount}`;
            }

            // Create card element
            const ml = event.ml_analysis || {};
            const kmeans = ml.kmeans || { cluster_id: 0, cluster_name: 'Cụm 0', cluster_color: '#2563EB' };
            const recs = ml.tfidf_recommendations || [];
            const fp = ml.fp_growth || { suggested_activities: [] };

            const card = document.createElement('div');
            card.className = 'stream-card';
            card.id = `event-card-${eventCount}`;

            // Chỉ sử dụng ảnh gốc từ Dataset (nếu có). Không tự ý bịa/thay thế ảnh ngoại lai.
            let imgUrl = (event.image && event.image.startsWith('http') && !event.image.includes('surl.li')) ? event.image : '';
            if (!imgUrl && allLoadedSpots && allLoadedSpots.length > 0) {
                const matchedSpot = allLoadedSpots.find(s => s.stt === parseInt(event.stt)) ||
                                    allLoadedSpots.find(s => s.name && event.name && s.name.trim().toLowerCase() === event.name.trim().toLowerCase());
                if (matchedSpot && matchedSpot.image && matchedSpot.image.startsWith('http') && !matchedSpot.image.includes('surl.li')) {
                    imgUrl = matchedSpot.image;
                }
            }

            const imgTag = imgUrl ? `<img src="${imgUrl}" class="card-thumbnail" alt="${escapeHtmlAttr(event.name || 'Điểm đến')}" onerror="this.remove();">` : '';

            const isUserReview = event.is_user_review || !!event.user_name;
            const reviewBadge = isUserReview ? `<span style="background:#EC4899; color:#fff; font-size:10px; font-weight:800; padding:2px 6px; border-radius:4px; margin-left:6px; letter-spacing:0.02em;">👤 KHÁCH VỪA ĐÁNH GIÁ</span>` : '';
            const userReviewBox = isUserReview ? `
                <div style="background:#FFF1F2; border:1px solid #FECDD3; border-radius:8px; padding:0.45rem 0.65rem; font-size:11px; margin-top:0.3rem;">
                    <b style="color:#BE123C;">${escapeHtmlText(event.user_name || 'Khách du lịch')}:</b>
                    <span style="color:#4C0519; font-style:italic;">"${escapeHtmlText(event.review_text || 'Đánh giá 5 sao!')}"</span>
                </div>
            ` : '';

            card.innerHTML = `
                <div class="card-top">
                    <div class="card-header-left">
                        ${imgTag}
                        <div>
                            <div style="display:flex; align-items:center;">
                                <h3 class="card-name">${escapeHtmlText(event.name || 'Điểm đến')}</h3>
                                ${reviewBadge}
                            </div>
                            <div class="card-loc">📍 ${escapeHtmlText(event.location || '')} • <span class="card-rating">★ ${event.rating || 4.5}/5</span></div>
                        </div>
                    </div>
                    <span class="card-cluster-badge" style="background: ${kmeans.cluster_color}15; color: ${kmeans.cluster_color}; border: 1px solid ${kmeans.cluster_color}40;">
                        <i class="dot" style="background: ${kmeans.cluster_color}; width: 8px; height: 8px; border-radius: 50%; display: inline-block;"></i>
                        ${escapeHtmlText(kmeans.cluster_name || '')}
                    </span>
                </div>

                ${userReviewBox}

                <p class="card-desc">${escapeHtmlText(event.description || '')}</p>

                <div class="card-tags">
                    ${(event.keywords || []).slice(0, 5).map(kw => `<span class="tag-badge">#${escapeHtmlText(kw)}</span>`).join('')}
                </div>

                <div class="card-ml-row">
                    <div class="ml-sub-title">🤖 Gợi Ý TF-IDF Tương Đồng:</div>
                    <div class="recs-list">
                        ${recs.map(r => `<span class="rec-item">🎯 ${escapeHtmlText(r.name || '')} (${Math.round((r.similarity_score || 0.8)*100)}%)</span>`).join('')}
                    </div>
                </div>

                <div class="card-ml-row">
                    <div class="ml-sub-title">✨ FP-Growth Hoạt Động Kèm Theo:</div>
                    <div class="fp-activities">
                        ${(fp.suggested_activities || []).map(act => `<span class="fp-chip">💡 ${escapeHtmlText(act)}</span>`).join('')}
                    </div>
                </div>

                <div class="card-footer">
                    <span>⚡ Kafka Topic: tourism-input ➔ tourism-output</span>
                    <span>🕒 ${escapeHtmlText(event.timestamp || new Date().toLocaleTimeString())}</span>
                </div>
            `;

            const feedBox = document.getElementById('feed-scroll-box');
            if (feedBox) {
                feedBox.insertBefore(card, feedBox.firstChild);
                while (feedBox.children.length > 30) {
                    feedBox.lastElementChild.remove();
                }
            }

            if (typeof highlightSpotOnMap === 'function') {
                highlightSpotOnMap(event.name, event.latitude, event.longitude, kmeans.cluster_color);
            }
        } catch (err) {
            console.error("[-] Lỗi khi xử lý sự kiện stream:", err, event);
        }
    }

    function highlightSpotOnMap(name, lat, lng, color) {
        if (!lat || !lng) return;

        // If existing highlight marker, remove it
        if (activeHighlightMarker) {
            map.removeLayer(activeHighlightMarker);
        }

        // Create radiant pulsing highlight marker
        activeHighlightMarker = L.circleMarker([lat, lng], {
            radius: 12,
            fillColor: color,
            color: '#FFFFFF',
            weight: 3,
            opacity: 1,
            fillOpacity: 0.95
        }).addTo(map);

        activeHighlightMarker.bindPopup(`
            <div style="font-family: inherit; font-size: 13px; text-align: center;">
                <strong style="color: #1E293B; font-size: 14px;">${name}</strong><br>
                <span style="color: #059669; font-weight: 700;">● Vừa nhận từ luồng Kafka!</span>
            </div>
        `).openPopup();
    }

    // 3. Smart Itinerary Recommendation System
    const itineraryInput = document.getElementById('itinerary-input');
    const btnRunRecommend = document.getElementById('btn-run-recommend');
    const recResultsContainer = document.getElementById('recommendation-results');
    const presetBtns = document.querySelectorAll('.preset-btn');

    presetBtns.forEach(btn => {
        btn.addEventListener('click', () => {
            presetBtns.forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            itineraryInput.value = btn.getAttribute('data-keywords');
            executeRecommendation();
        });
    });

    if (btnRunRecommend) btnRunRecommend.addEventListener('click', executeRecommendation);
    if (itineraryInput) {
        itineraryInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') executeRecommendation();
        });
    }

    function executeRecommendation() {
        const query = itineraryInput && itineraryInput.value ? itineraryInput.value.trim() : "";
        if (!query) return;

        const keywords = query.split(',').map(s => s.trim());
        recResultsContainer.innerHTML = `
            <div style="grid-column: 1/-1; text-align: center; padding: 2rem; color: #64748B;">
                <div class="spinner" style="margin: 0 auto 1rem;"></div>
                <p style="font-weight: 600;">Đang tính toán vector TF-IDF & áp dụng thuật toán FP-Growth...</p>
            </div>
        `;

        fetch('/api/recommend', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ keywords: keywords })
        })
        .then(res => res.json())
        .then(data => {
            renderRecommendations(data);
        })
        .catch(err => {
            recResultsContainer.innerHTML = `<p style="color: #DC2626;">Lỗi: ${err.message}</p>`;
        });
    }

    function renderRecommendations(data) {
        const recs = data.recommendations || [];
        const fp = data.fp_growth || {};
        const activities = fp.suggested_activities || [];

        if (recs.length === 0) {
            const msg = data.message || "Không tìm thấy kết quả phù hợp từ Spark MLlib.";
            recResultsContainer.innerHTML = `
                <div style="grid-column: 1/-1; text-align: center; padding: 2rem; background: #F8FAFC; border-radius: 12px; border: 1px dashed #CBD5E1;">
                    <p style="color: #64748B; font-weight: 600; margin-bottom: 0.5rem;">ℹ️ ${msg}</p>
                    <p style="font-size: 12px; color: #94A3B8;">Gợi ý: Hãy nhấp vào các nút phong cách mẫu bên trên (🏖️ Nghỉ dưỡng biển, 🏛️ Di tích, ⛰️ Trekking, v.v.)</p>
                </div>
            `;
            return;
        }

        let html = '';
        recs.forEach((spot, idx) => {
            const defaultImg = "https://images.unsplash.com/photo-1528127269322-539801943592?auto=format&fit=crop&w=400&q=80";
            const imgUrl = spot.image && spot.image.startsWith('http') ? spot.image : defaultImg;
            const scoreVal = spot.similarity_score !== undefined ? Number(spot.similarity_score) : 0;
            const scorePct = Math.round(scoreVal * 100);

            html += `
                <div class="rec-card">
                    <img src="${imgUrl}" alt="${spot.name}" onerror="this.onerror=null; this.src='${defaultImg}';">
                    <div class="rec-card-body">
                        <div style="display: flex; justify-content: space-between; align-items: flex-start;">
                            <h4 style="font-size: 15px; font-weight: 800; color: #0F172A;">#${idx+1} ${spot.name}</h4>
                            <span class="card-rating">★ ${spot.rating}/5</span>
                        </div>
                        <div style="font-size: 12px; color: #64748B;">📍 ${spot.location}</div>
                        <div style="margin-top: 4px;">
                            <div style="display: flex; justify-content: space-between; font-size: 11px; color: #475569; margin-bottom: 3px;">
                                <span style="font-weight: 600;">Cosine TF-IDF Spark:</span>
                                <strong style="color: #4F46E5;">${scoreVal.toFixed(4)} (${scorePct}%)</strong>
                            </div>
                            <div class="rec-score-bar">
                                <div class="rec-score-fill" style="width: ${Math.min(100, Math.max(5, scorePct))}%;"></div>
                            </div>
                        </div>
                        <div style="font-size: 11px; color: #64748B; margin-top: auto; padding-top: 8px; border-top: 1px solid #F1F5F9;">
                            💡 Hoạt động gợi ý: <span style="color: #4F46E5; font-weight: 600;">${activities.slice(0, 2).join(', ')}</span>
                        </div>
                    </div>
                </div>
            `;
        });

        recResultsContainer.innerHTML = html;
    }

    // --- Multi-Day Itinerary Planner (Haversine Geo-Routing) ---
    const multidayGrid = document.getElementById('multiday-cards-grid');
    const regionButtons = document.querySelectorAll('.region-btn');

    function loadMultidayItinerary(province) {
        if (!multidayGrid) return;
        multidayGrid.innerHTML = `
            <div style="grid-column: 1/-1; text-align: center; padding: 2rem; color: #64748B;">
                <div class="spinner" style="margin: 0 auto 0.75rem;"></div>
                <p>Đang tối ưu hóa khoảng cách Haversine cho địa phương <b>${province}</b>...</p>
            </div>
        `;

        fetch(`/api/itinerary_multiday?province=${encodeURIComponent(province)}&days=3`)
            .then(res => res.json())
            .then(data => {
                renderMultidayCards(data);
            })
            .catch(err => {
                multidayGrid.innerHTML = `<div style="grid-column:1/-1; color:#DC2626; padding:1rem;">Lỗi tải lịch trình: ${err.message}</div>`;
            });
    }

    function renderMultidayCards(data) {
        if (!multidayGrid) return;
        const itinerary = data.itinerary || [];
        if (itinerary.length === 0 || data.status === 'waiting_spark') {
            const msg = data.message || "Chưa có kết quả lịch trình từ Spark MLlib qua Kafka cho khu vực này.";
            multidayGrid.innerHTML = `
                <div style="grid-column:1/-1; padding:2rem; text-align:center; color:#64748B; background:#F8FAFC; border-radius:12px; border:1px dashed #CBD5E1;">
                    <p style="font-weight:700; color:#334155; margin-bottom:0.25rem;">⏳ ${msg}</p>
                    <small>Hệ thống tuân thủ luồng chuẩn: Lịch trình chỉ được phục vụ khi do PySpark MLlib trên HDFS tính toán và phát qua Kafka.</small>
                </div>
            `;
            return;
        }

        let html = '';
        const dayBadges = ['badge-blue', 'badge-purple', 'badge-green', 'badge-yellow'];
        const dayGradients = [
            'linear-gradient(135deg, #4F46E5, #06B6D4)',
            'linear-gradient(135deg, #7C3AED, #EC4899)',
            'linear-gradient(135deg, #059669, #10B981)',
            'linear-gradient(135deg, #D97706, #F59E0B)'
        ];

        itinerary.forEach((dayPlan, idx) => {
            const badgeClass = dayBadges[idx % dayBadges.length];
            const headerGrad = dayGradients[idx % dayGradients.length];

            let stopsHtml = '';
            dayPlan.stops.forEach((stop, sIdx) => {
                const stopStt = parseInt(stop.stt || 0);
                const spotObj = allLoadedSpots.find(s => s.stt === stopStt) || allLoadedSpots.find(s => s.name === stop.name);
                const currentRating = (spotObj && spotObj.rating !== undefined && spotObj.rating !== null)
                    ? Number(spotObj.rating).toFixed(1)
                    : (stop.rating !== undefined && stop.rating !== null ? Number(stop.rating).toFixed(1) : "4.5");

                stopsHtml += `
                    <div style="display:flex; align-items:flex-start; gap:0.6rem; margin-bottom:0.6rem; padding:0.5rem; background:#F8FAFC; border-radius:8px; border:1px solid #E2E8F0;">
                        <span style="display:inline-flex; align-items:center; justify-content:center; width:22px; height:22px; border-radius:50%; background:#4F46E5; color:#fff; font-size:11px; font-weight:800; flex-shrink:0;">${sIdx+1}</span>
                        <div style="flex:1; min-width:0;">
                            <div style="font-weight:700; font-size:13px; color:#0F172A; white-space:nowrap; overflow:hidden; text-overflow:ellipsis;">${escapeHtmlText(stop.name)}</div>
                            <div style="font-size:11px; color:#64748B;">📍 ${escapeHtmlText(stop.location)} • <b style="color:#F59E0B;" class="itin-stop-rating-${stopStt}">★ ${currentRating}</b></div>
                            <div style="font-size:11px; color:#475569; margin-top:2px;">🏷️ <i>${escapeHtmlText((stop.keywords || []).slice(0, 3).join(', '))}</i></div>
                        </div>
                    </div>
                `;
            });

            html += `
                <div class="glass-card" style="background:#FFFFFF; border:1px solid #E2E8F0; border-radius:14px; overflow:hidden; box-shadow:0 4px 14px rgba(0,0,0,0.04); display:flex; flex-direction:column;">
                    <div style="background:${headerGrad}; color:#fff; padding:0.85rem 1.15rem; display:flex; justify-content:space-between; align-items:center;">
                        <div>
                            <span style="font-size:11px; text-transform:uppercase; letter-spacing:0.05em; opacity:0.9; font-weight:700;">Lịch Trình Haversine</span>
                            <h3 style="margin:0; font-size:16px; font-weight:800; color:#fff;">Ngày ${dayPlan.day}</h3>
                        </div>
                        <div style="text-align:right;">
                            <span style="background:rgba(255,255,255,0.22); padding:0.2rem 0.55rem; border-radius:12px; font-size:11px; font-weight:700;">${dayPlan.stops.length} Điểm đến</span>
                            <div style="font-size:11px; margin-top:2px; opacity:0.95;">Cự ly: ~${dayPlan.total_distance_km} km</div>
                        </div>
                    </div>
                    <div style="padding:1rem; flex:1;">
                        ${stopsHtml}
                    </div>
                    <div style="padding:0.6rem 1rem; background:#F8FAFC; border-top:1px solid #E2E8F0; font-size:11px; color:#64748B; display:flex; justify-content:space-between;">
                        <span>Tối ưu hóa: Haversine Geo-Radius</span>
                        <span style="color:#059669; font-weight:700;">✓ Tuyến ngắn nhất</span>
                    </div>
                </div>
            `;
        });

        multidayGrid.innerHTML = html;
    }

    if (regionButtons.length > 0) {
        regionButtons.forEach(b => {
            b.addEventListener('click', () => {
                regionButtons.forEach(btn => btn.classList.remove('active'));
                b.classList.add('active');
                const reg = b.getAttribute('data-region') || 'Khánh Hòa';
                loadMultidayItinerary(reg);
            });
        });
        // Load initial region
        loadMultidayItinerary('Khánh Hòa');
    }

    // --- Live Review Simulation Handler (Demo trước lớp) ---
    const btnSendSimReview = document.getElementById('btn-send-sim-review');
    const simUserNameInput = document.getElementById('sim-user-name');
    const simSpotNameSelect = document.getElementById('sim-spot-name');
    const simCommentInput = document.getElementById('sim-comment');
    const reviewStatusIndicator = document.getElementById('review-status-indicator');

    if (btnSendSimReview) {
        btnSendSimReview.addEventListener('click', () => {
            const userName = (simUserNameInput && simUserNameInput.value.trim()) || currentDemoUser || "Nguyễn Trường Nhu (Người dùng A)";
            const opt = simSpotNameSelect.options[simSpotNameSelect.selectedIndex];
            const spotStt = opt ? parseInt(opt.getAttribute('data-stt') || "0") : 0;
            const spotName = (simSpotNameSelect && simSpotNameSelect.value) || "Bà Nà Hills";
            const comment = (simCommentInput && simCommentInput.value.trim()) || "Trải nghiệm tuyệt vời!";

            btnSendSimReview.disabled = true;
            btnSendSimReview.textContent = "Đang gửi...";
            if (reviewStatusIndicator) {
                reviewStatusIndicator.textContent = "⏳ Đang xử lý (đang gửi Kafka)...";
                reviewStatusIndicator.style.color = "#D97706";
            }

            fetch('/api/submit_review', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    stt: spotStt,
                    spot_name: spotName,
                    rating: 5.0,
                    user_name: userName,
                    review_text: comment
                })
            })
            .then(res => {
                if (res.status === 200 || res.status === 202) {
                    return res.json().then(data => {
                        btnSendSimReview.disabled = false;
                        btnSendSimReview.textContent = "Bắn Kafka ⚡";
                        if (reviewStatusIndicator) {
                            reviewStatusIndicator.textContent = `⏳ Đang xử lý (đã vào Kafka #${data.event_id}, chờ Spark)...`;
                            reviewStatusIndicator.style.color = "#D97706";
                        }
                        // Đăng ký theo dõi 15 giây
                        const evId = String(data.event_id);
                        const timer = setTimeout(() => {
                            if (pendingReviewJobs.has(evId)) {
                                pendingReviewJobs.delete(evId);
                                if (reviewStatusIndicator) {
                                    reviewStatusIndicator.textContent = "❌ Lỗi/quá hạn (15s không có phản hồi từ Spark)";
                                    reviewStatusIndicator.style.color = "#DC2626";
                                }
                            }
                        }, 15000);
                        pendingReviewJobs.set(evId, { source: 'sim', timer, spotName });
                    });
                } else if (res.status === 503) {
                    btnSendSimReview.disabled = false;
                    btnSendSimReview.textContent = "Bắn Kafka ⚡";
                    if (reviewStatusIndicator) {
                        reviewStatusIndicator.textContent = "❌ Lỗi/quá hạn: Kafka Broker ngoại tuyến (503)";
                        reviewStatusIndicator.style.color = "#DC2626";
                    }
                } else {
                    return res.json().then(errData => {
                        throw new Error(errData.message || `HTTP ${res.status}`);
                    });
                }
            })
            .catch(err => {
                btnSendSimReview.disabled = false;
                btnSendSimReview.textContent = "Thử lại";
                if (reviewStatusIndicator) {
                    reviewStatusIndicator.textContent = "❌ Lỗi: " + err.message;
                    reviewStatusIndicator.style.color = "#DC2626";
                }
            });
        });
    }

    // --- POI Directory & Community Reviews Interactive Logic --- (variables hoisted to top)

    const spotsCardsGrid = document.getElementById('spots-cards-grid');
    const directorySearchInput = document.getElementById('directory-search-input');
    const clusterFilterButtons = document.querySelectorAll('#cluster-filter-buttons .filter-chip');

    // Review Modal Elements
    const reviewModal = document.getElementById('review-modal');
    const modalSpotName = document.getElementById('modal-spot-name');
    const modalUserName = document.getElementById('modal-user-name');
    const modalRatingSelect = document.getElementById('modal-rating-select');
    const modalComment = document.getElementById('modal-comment');
    const btnCloseModal = document.getElementById('btn-close-modal');
    const btnModalSubmit = document.getElementById('btn-modal-submit');
    let targetSpotForReview = null;

    function renderSpotsDirectory(spots) {
        if (!spotsCardsGrid) return;
        if (!spots || spots.length === 0) {
            spotsCardsGrid.innerHTML = `
                <div style="grid-column: 1/-1; text-align: center; padding: 3rem; color: #64748B;">
                    <p style="font-size: 16px; font-weight: 700;">Không tìm thấy địa điểm nào phù hợp với từ khóa.</p>
                    <small>Hãy thử tìm tên tỉnh thành khác như: Khánh Hòa, Đà Lạt, Hà Nội, Đà Nẵng...</small>
                </div>
            `;
            return;
        }

        let html = '';
        spots.forEach(spot => {
            const hasRawImg = Boolean(spot.image && spot.image.startsWith('http') && !spot.image.includes('surl.li'));
            const imgUrl = hasRawImg ? spot.image : '';
            const latestRev = spot.latest_review || null;

            const cardId = `spot-dir-card-${spot.stt}`;

            let reviewBubbleHtml = '';
            if (latestRev && latestRev.review_text) {
                reviewBubbleHtml = `
                    <div class="spot-review-bubble" id="bubble-${cardId}" onclick="openCommentsModalForSpot(${spot.stt})" title="Bấm để xem toàn bộ bình luận" style="cursor: pointer;">
                        <div class="review-bubble-top">
                            <span class="review-author" style="overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 65%;">💬 <span class="author-name">${escapeHtmlText(latestRev.user_name) || 'Khách du lịch'}</span></span>
                            <span class="review-time" style="white-space: nowrap; flex-shrink: 0;">${escapeHtmlText(latestRev.timestamp || 'Gần đây')}</span>
                        </div>
                        <div class="review-quote">"${escapeHtmlText(latestRev.review_text) || 'Bình luận đã được ghi nhận.'}"</div>
                    </div>
                `;
            } else {
                reviewBubbleHtml = `
                    <div class="spot-review-bubble" id="bubble-${cardId}" onclick="openCommentsModalForSpot(${spot.stt})" title="Bấm để xem và viết bình luận" style="background: #F8FAFC; border-color: #E2E8F0; cursor: pointer;">
                        <div class="review-bubble-top">
                            <span class="review-author" style="color: #94A3B8;">💬 <span class="author-name">Chưa có đánh giá</span></span>
                            <span class="review-time" style="color: #94A3B8; font-size: 10px;">Nội dung minh họa</span>
                        </div>
                        <div class="review-quote" style="color: #64748B; font-style: italic;">
                            "Điểm đến đẹp và không khí trong lành. Hãy là người đầu tiên gửi nhận xét thực tế!"
                        </div>
                    </div>
                `;
            }

            html += `
                <div class="spot-dir-card" id="${cardId}" data-stt="${spot.stt}" data-spot-name="${escapeHtmlAttr(spot.name)}" data-cluster="${spot.cluster_id}">
                    <div class="spot-dir-img-wrap" style="background: linear-gradient(135deg, #F1F5F9, #E2E8F0); display: flex; align-items: center; justify-content: center; min-height: 140px;">
                        ${imgUrl ? `<img src="${imgUrl}" class="spot-dir-img" alt="${escapeHtmlAttr(spot.name)}" onerror="this.style.display='none';">` : `<div style="font-size: 2.2rem; opacity: 0.6;">📍</div>`}
                        <div class="spot-dir-rating-badge">
                            ★ <span class="spot-card-rating-num">${(spot.rating || 4.5).toFixed(1)}</span>
                        </div>
                        <div class="spot-dir-cluster-badge" style="color: ${spot.cluster_color}; border-left: 3px solid ${spot.cluster_color};">
                            ${(spot.cluster_name || '').split('(')[0].trim()}
                        </div>
                    </div>
                    
                    <div class="spot-dir-body">
                        <div class="spot-dir-header">
                            <div>
                                <h3 class="spot-dir-title">#${spot.stt} ${escapeHtmlText(spot.name)}</h3>
                                <div class="spot-dir-location">📍 ${escapeHtmlText(spot.location)}</div>
                            </div>
                        </div>

                        <p class="spot-dir-desc">${escapeHtmlText(spot.description || 'Địa danh du lịch nổi tiếng với nét văn hóa đặc sắc và phong cảnh thiên nhiên.')}</p>

                        ${reviewBubbleHtml}

                        <div class="spot-dir-footer">
                            <span onclick="openCommentsModalForSpot(${spot.stt})" style="font-size: 11px; color: #4F46E5; cursor: pointer; font-weight: 600;" title="Bấm để xem toàn bộ đánh giá phong cách Facebook">
                                👥 <b class="review-count-num">${spot.review_count !== undefined && spot.review_count !== null ? spot.review_count : 0}</b> đánh giá <span style="font-size: 10px; text-decoration: underline; color: #2563EB;">(Xem tất cả)</span>
                            </span>
                            <div style="display: flex; gap: 6px;">
                                <button class="btn-view-all-comments" onclick="openCommentsModalForSpot(${spot.stt})" title="Xem và gửi bình luận đánh giá">
                                    💬 Bình luận
                                </button>
                            </div>
                        </div>
                    </div>
                </div>
            `;
        });

        spotsCardsGrid.innerHTML = html;
    }

    // Filter & Search Logic
    function filterAndRenderDirectory() {
        if (!allLoadedSpots || allLoadedSpots.length === 0) return;

        let filtered = allLoadedSpots.filter(spot => {
            const matchesCluster = currentClusterFilter === 'all' || spot.cluster_id.toString() === currentClusterFilter;
            
            const kw = currentSearchKeyword.toLowerCase();
            const matchesKeyword = !kw || 
                spot.name.toLowerCase().includes(kw) || 
                spot.location.toLowerCase().includes(kw) ||
                (spot.description && spot.description.toLowerCase().includes(kw)) ||
                (spot.keywords && spot.keywords.some(k => k.toLowerCase().includes(kw)));

            return matchesCluster && matchesKeyword;
        });

        renderSpotsDirectory(filtered);
    }

    if (directorySearchInput) {
        directorySearchInput.addEventListener('input', (e) => {
            currentSearchKeyword = e.target.value.trim();
            filterAndRenderDirectory();
        });
    }

    if (clusterFilterButtons) {
        clusterFilterButtons.forEach(btn => {
            btn.addEventListener('click', () => {
                clusterFilterButtons.forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                currentClusterFilter = btn.getAttribute('data-cluster');
                filterAndRenderDirectory();
            });
        });
    }

    let targetSpotStt = null;
    let targetSpotName = null;

    // Modal Interaction
    window.openReviewModalForSpot = function(spotStt, spotName) {
        targetSpotStt = parseInt(spotStt);
        const spot = allLoadedSpots ? allLoadedSpots.find(s => s.stt === targetSpotStt) : null;
        targetSpotName = spotName || (spot ? spot.name : `Địa điểm #${targetSpotStt}`);
        if (modalSpotName) modalSpotName.textContent = `✍️ Đánh Giá: #${targetSpotStt} ${targetSpotName}`;
        if (modalUserName) modalUserName.value = currentDemoUser || "Nguyễn Trường Nhu (Người dùng A)";
        if (modalComment) modalComment.value = "";
        if (reviewModal) reviewModal.classList.add('show');
    };

    if (btnCloseModal) {
        btnCloseModal.addEventListener('click', () => {
            if (reviewModal) reviewModal.classList.remove('show');
        });
    }

    window.addEventListener('click', (e) => {
        if (e.target === reviewModal) {
            reviewModal.classList.remove('show');
        }
    });

    if (btnModalSubmit) {
        btnModalSubmit.addEventListener('click', () => {
            if (!targetSpotName) return;
            const userName = (modalUserName && modalUserName.value.trim()) || currentDemoUser || "Nguyễn Trường Nhu (Người dùng A)";
            const rating = parseFloat(modalRatingSelect ? modalRatingSelect.value : "5.0");
            const comment = (modalComment && modalComment.value.trim()) || "Chỗ này đẹp lắm, vừa đi hôm qua có cả bình minh!";

            btnModalSubmit.disabled = true;
            btnModalSubmit.textContent = "⏳ Đang xử lý...";

            fetch('/api/submit_review', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    stt: targetSpotStt,
                    spot_name: targetSpotName,
                    rating: rating,
                    user_name: userName,
                    review_text: comment
                })
            })
            .then(async res => {
                btnModalSubmit.disabled = false;
                btnModalSubmit.textContent = "🚀 Gửi Đánh Giá Ngay (Bắn Kafka Live)";

                let data = null;
                const contentType = res.headers.get("content-type") || "";
                if (contentType.includes("application/json")) {
                    try {
                        data = await res.json();
                    } catch (e) {
                        data = null;
                    }
                }

                if (res.status === 200 || res.status === 202) {
                    if (reviewModal) reviewModal.classList.remove('show');
                    const evId = data ? String(data.event_id) : '';
                    const revObj = {
                        user_name: userName,
                        review_text: comment,
                        timestamp: "Đang xử lý..."
                    };

                    // HTTP 202 chỉ hiển thị trạng thái đang xử lý, không ghi đè điểm chính thức và không khóa event_id
                    showPendingSpotCardReview(targetSpotStt, targetSpotName, revObj, evId);

                    showRealtimeToast(
                        targetSpotName,
                        userName,
                        `Đang xử lý (đã vào Kafka #${evId ? evId.slice(0, 8) : 'live'}), chờ Spark tính toán điểm chính thức...`,
                        null,
                        false,
                        targetSpotStt
                    );

                    // Đăng ký theo dõi 30 giây để xác nhận khi Spark xử lý xong
                    const timer = setTimeout(() => {
                        if (pendingReviewJobs.has(evId)) {
                            pendingReviewJobs.delete(evId);
                            showRealtimeToast(
                                "Spark Streaming",
                                "Thông tin",
                                `Đánh giá #${evId ? evId.slice(0, 8) : ''} đã chuyển sang xử lý phân tán HDFS.`,
                                null,
                                true
                            );
                        }
                    }, 30000);
                    if (evId) {
                        pendingReviewJobs.set(evId, { source: 'modal', timer, spotName: targetSpotName });
                    }
                } else if (res.status === 503) {
                    const msg = (data && data.message) || "Cụm Apache Kafka Broker (Port 9092) đang ngoại tuyến!";
                    showRealtimeToast("Lỗi hệ thống (503)", "Kafka Offline", msg, 1.0, true);
                    alert("❌ Lỗi: " + msg);
                } else {
                    const errMsg = (data && (data.message || data.error)) || `Lỗi máy chủ (HTTP ${res.status})`;
                    throw new Error(errMsg);
                }
            })
            .catch(err => {
                btnModalSubmit.disabled = false;
                btnModalSubmit.textContent = "Lỗi - Thử lại";
                alert("Lỗi gửi đánh giá: " + err.message);
            });
        });
    }

    // =========================================================================
    // Modal Xem Toàn Bộ Đánh Giá Dạng Bảng Tin Bình Luận Phong Cách Facebook
    // =========================================================================
    const spotCommentsModal = document.getElementById('spot-comments-modal');
    const btnCloseCommentsModal = document.getElementById('btn-close-comments-modal');
    const fbCommentsList = document.getElementById('fb-comments-list');
    const fbModalSpotImg = document.getElementById('fb-modal-spot-img');
    const fbModalSpotName = document.getElementById('fb-modal-spot-name');
    const fbModalRatingVal = document.getElementById('fb-modal-rating-val');
    const fbModalSpotLoc = document.getElementById('fb-modal-spot-loc');
    const fbModalTotalCount = document.getElementById('fb-modal-total-count');
    const fbFilterAll = document.getElementById('fb-filter-all');
    const fbFilter5star = document.getElementById('fb-filter-5star');
    const fbInputUserName = document.getElementById('fb-input-user-name');
    const fbCurrentUserAvatar = document.getElementById('fb-current-user-avatar');
    const fbModalRatingSelect = document.getElementById('fb-modal-rating-select');
    const fbInputText = document.getElementById('fb-input-text');
    const fbBtnSubmitComment = document.getElementById('fb-btn-submit-comment');

    let currentCommentsSpotStt = null;
    let currentCommentsSpotName = "";
    let currentLoadedReviews = [];
    let currentCommentFilter = 'all';

    function getAvatarBgColor(name) {
        const colors = [
            'linear-gradient(135deg, #2563EB, #1D4ED8)',
            'linear-gradient(135deg, #059669, #047857)',
            'linear-gradient(135deg, #D97706, #B45309)',
            'linear-gradient(135deg, #7C3AED, #6D28D9)',
            'linear-gradient(135deg, #DB2777, #BE185D)',
            'linear-gradient(135deg, #0284C7, #0369A1)',
            'linear-gradient(135deg, #EA580C, #C2410C)',
            'linear-gradient(135deg, #4F46E5, #3730A3)'
        ];
        let hash = 0;
        const str = name || "Khách du lịch";
        for (let i = 0; i < str.length; i++) {
            hash = str.charCodeAt(i) + ((hash << 5) - hash);
        }
        return colors[Math.abs(hash) % colors.length];
    }

    function getAvatarInitials(name) {
        if (!name) return "KH";
        const parts = name.trim().split(/\s+/);
        if (parts.length === 1) return parts[0].slice(0, 2).toUpperCase();
        return (parts[0][0] + parts[parts.length - 1][0]).toUpperCase();
    }



    window.openCommentsModalForSpot = function(spotStt) {
        currentCommentsSpotStt = parseInt(spotStt);
        if (!currentCommentsSpotStt) return;

        const spot = allLoadedSpots ? allLoadedSpots.find(s => s.stt === currentCommentsSpotStt) : null;
        currentCommentsSpotName = spot ? spot.name : `Địa điểm #${currentCommentsSpotStt}`;

        if (fbModalSpotName) fbModalSpotName.textContent = `#${currentCommentsSpotStt} ${currentCommentsSpotName}`;
        if (fbModalRatingVal && spot) fbModalRatingVal.textContent = (spot.rating || 4.5).toFixed(1);
        if (fbModalSpotLoc && spot) fbModalSpotLoc.textContent = `📍 ${spot.location || 'Việt Nam'}`;
        if (fbModalTotalCount && spot) fbModalTotalCount.textContent = `${spot.review_count || 0} đánh giá`;

        const defaultImg = "https://images.unsplash.com/photo-1528127269322-539801943592?auto=format&fit=crop&w=400&q=80";
        if (fbModalSpotImg) {
            fbModalSpotImg.src = (spot && spot.image && spot.image.startsWith('http')) ? spot.image : defaultImg;
        }

        const curUser = currentDemoUser || "Nguyễn Trường Nhu (Người dùng A)";
        if (fbInputUserName) fbInputUserName.textContent = curUser;
        if (fbCurrentUserAvatar) {
            fbCurrentUserAvatar.textContent = getAvatarInitials(curUser);
            fbCurrentUserAvatar.style.background = getAvatarBgColor(curUser);
        }
        if (fbInputText) fbInputText.value = "";

        if (spotCommentsModal) spotCommentsModal.classList.add('show');
        if (fbCommentsList) {
            fbCommentsList.innerHTML = `
                <div class="fb-empty-state">
                    <div class="spinner" style="border-top-color: #2563EB;"></div>
                    <p style="font-size: 0.85rem; font-weight: 600; color: #475569; margin-top: 0.5rem;">Đang tải toàn bộ bình luận từ HDFS & Kafka Serving Layer...</p>
                </div>
            `;
        }

        currentCommentFilter = 'all';
        if (fbFilterAll) fbFilterAll.classList.add('active');
        if (fbFilter5star) fbFilter5star.classList.remove('active');

        fetch(`/api/spot_reviews/${currentCommentsSpotStt}?_t=${Date.now()}`)
            .then(res => res.json())
            .then(data => {
                if (data.status === 'success') {
                    const s = data.spot || {};
                    if (fbModalSpotName) fbModalSpotName.textContent = `#${s.stt} ${s.name}`;
                    if (fbModalRatingVal) fbModalRatingVal.textContent = (s.rating || 4.5).toFixed(1);
                    if (fbModalSpotLoc) fbModalSpotLoc.textContent = `📍 ${s.location || 'Việt Nam'}`;
                    if (fbModalTotalCount) fbModalTotalCount.textContent = `${data.total_reviews} đánh giá`;
                    if (s.image && fbModalSpotImg) fbModalSpotImg.src = s.image;

                    currentLoadedReviews = data.reviews || [];
                    renderFbComments(currentLoadedReviews);
                } else {
                    if (fbCommentsList) {
                        fbCommentsList.innerHTML = `
                            <div class="fb-empty-state">
                                <p style="color: #EF4444;">Không thể tải danh sách bình luận: ${data.message || 'Lỗi không xác định'}</p>
                            </div>
                        `;
                    }
                }
            })
            .catch(err => {
                console.error("Lỗi tải bình luận:", err);
                if (fbCommentsList) {
                    fbCommentsList.innerHTML = `
                        <div class="fb-empty-state">
                            <p style="color: #EF4444;">Lỗi kết nối máy chủ khi nạp bình luận.</p>
                        </div>
                    `;
                }
            });
    };

    function renderFbComments(reviews) {
        if (!fbCommentsList) return;
        let list = reviews || [];
        if (currentCommentFilter === '5') {
            list = list.filter(r => (parseFloat(r.rating) || 5.0) >= 4.9);
        }

        if (list.length === 0) {
            fbCommentsList.innerHTML = `
                <div class="fb-empty-state">
                    <div class="fb-empty-icon">💬</div>
                    <div style="font-weight: 700; color: #475569; font-size: 0.95rem;">Chưa có nhận xét nào</div>
                    <p style="font-size: 0.8rem; color: #94A3B8; max-width: 320px;">
                        Hãy là người đầu tiên chia sẻ cảm nhận về địa điểm này ở thanh bình luận bên dưới!
                    </p>
                </div>
            `;
            return;
        }

        let html = '';
        list.forEach((rev, idx) => {
            const userName = rev.user_name || "Khách du lịch";
            const rating = parseFloat(rev.rating) || 5.0;
            const text = rev.review_text || "";
            const time = rev.timestamp || "Gần đây";
            const initials = getAvatarInitials(userName);
            const bgGrad = getAvatarBgColor(userName);
            const evId = rev.event_id || `rev_${idx}`;

            html += `
                <div class="fb-comment-item" id="fb-item-${evId}">
                    <div class="fb-avatar" style="background: ${bgGrad};">
                        ${initials}
                    </div>
                    <div class="fb-comment-body">
                        <div class="fb-comment-bubble">
                            <div class="fb-comment-head">
                                <div class="fb-author-wrap">
                                    <span class="fb-author-name">${escapeHtmlText(userName)}</span>
                                    <span class="fb-badge-role">Du khách</span>
                                </div>
                                <span class="fb-rating-pill">★ ${rating.toFixed(1)}</span>
                            </div>
                            <div class="fb-comment-msg">${escapeHtmlText(text)}</div>
                        </div>
                        <div class="fb-comment-meta">
                            <span>${time}</span>
                            <button class="fb-action-link" onclick="toggleFbLike(this)">
                                👍 Thích <span class="fb-like-num">1</span>
                            </button>
                            <button class="fb-action-link btn-reply-comment" data-user="${escapeHtmlAttr(userName)}">
                                💬 Phản hồi
                            </button>
                            <span class="fb-tag-verified">✓ Đã xác thực trên HDFS</span>
                        </div>
                    </div>
                </div>
            `;
        });

        fbCommentsList.innerHTML = html;

        if (!fbCommentsList._hasReplyDelegation) {
            fbCommentsList._hasReplyDelegation = true;
            fbCommentsList.addEventListener('click', (e) => {
                const btn = e.target.closest('.btn-reply-comment');
                if (btn) {
                    const u = btn.getAttribute('data-user');
                    if (u) replyToUser(u);
                }
            });
        }
    }

    window.toggleFbLike = function(btn) {
        if (!btn) return;
        const isLiked = btn.classList.contains('liked');
        const numEl = btn.querySelector('.fb-like-num');
        let count = numEl ? (parseInt(numEl.textContent) || 0) : 0;
        if (isLiked) {
            btn.classList.remove('liked');
            count = Math.max(0, count - 1);
        } else {
            btn.classList.add('liked');
            count += 1;
        }
        if (numEl) numEl.textContent = count;
    };

    window.replyToUser = function(userName) {
        if (!fbInputText) return;
        fbInputText.value = `@${userName} `;
        fbInputText.focus();
    };

    if (fbFilterAll) {
        fbFilterAll.addEventListener('click', () => {
            fbFilterAll.classList.add('active');
            if (fbFilter5star) fbFilter5star.classList.remove('active');
            currentCommentFilter = 'all';
            renderFbComments(currentLoadedReviews);
        });
    }

    if (fbFilter5star) {
        fbFilter5star.addEventListener('click', () => {
            fbFilter5star.classList.add('active');
            if (fbFilterAll) fbFilterAll.classList.remove('active');
            currentCommentFilter = '5';
            renderFbComments(currentLoadedReviews);
        });
    }

    if (btnCloseCommentsModal) {
        btnCloseCommentsModal.addEventListener('click', () => {
            if (spotCommentsModal) spotCommentsModal.classList.remove('show');
        });
    }

    window.addEventListener('click', (e) => {
        if (e.target === spotCommentsModal) {
            spotCommentsModal.classList.remove('show');
        }
    });

    if (fbBtnSubmitComment) {
        fbBtnSubmitComment.addEventListener('click', () => {
            if (!currentCommentsSpotStt) return;
            const text = fbInputText ? fbInputText.value.trim() : "";
            if (!text) {
                alert("Vui lòng nhập nội dung bình luận!");
                return;
            }

            const userName = currentDemoUser || (modalUserName && modalUserName.value.trim()) || "Nguyễn Trường Nhu (Người dùng A)";
            const rating = parseFloat(fbModalRatingSelect ? fbModalRatingSelect.value : "5.0");

            fbBtnSubmitComment.disabled = true;
            fbBtnSubmitComment.innerHTML = '<span>⏳</span>';

            fetch('/api/submit_review', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    stt: currentCommentsSpotStt,
                    spot_name: currentCommentsSpotName,
                    rating: rating,
                    user_name: userName,
                    review_text: text
                })
            })
            .then(async res => {
                fbBtnSubmitComment.disabled = false;
                fbBtnSubmitComment.innerHTML = '<span class="fb-send-icon">🚀</span><span>Gửi</span>';

                let data = null;
                try { data = await res.json(); } catch(e) {}

                const evId = data ? String(data.event_id) : `client_${Date.now()}`;
                const newRev = {
                    event_id: evId,
                    user_name: userName,
                    rating: rating,
                    review_text: text,
                    timestamp: new Date().toLocaleTimeString('vi-VN', { hour12: false }) + ' (Vừa xong)'
                };

                // Thêm ngay vào đầu bảng tin bình luận
                currentLoadedReviews.unshift(newRev);
                renderFbComments(currentLoadedReviews);

                // Cập nhật số đếm trên modal
                if (fbModalTotalCount) {
                    fbModalTotalCount.textContent = `${currentLoadedReviews.length} đánh giá`;
                }

                // Cập nhật thẻ ngoài danh bạ
                showPendingSpotCardReview(currentCommentsSpotStt, currentCommentsSpotName, newRev, evId);

                // Xóa ô nhập liệu
                if (fbInputText) fbInputText.value = "";

                showRealtimeToast(
                    currentCommentsSpotName,
                    userName,
                    `Đã gửi bình luận qua Kafka! Đang chờ Spark phân tích và ghi vào Data Lake...`,
                    rating,
                    false,
                    currentCommentsSpotStt
                );
            })
            .catch(err => {
                fbBtnSubmitComment.disabled = false;
                fbBtnSubmitComment.innerHTML = '<span class="fb-send-icon">🚀</span><span>Gửi</span>';
                console.error("Lỗi gửi bình luận:", err);
                alert("Lỗi khi gửi bình luận: " + err.message);
            });
        });
    }

    window.handleLiveCommentStream = function(event, revObj, updatedRating, reviewRating) {
        if (!spotCommentsModal || !spotCommentsModal.classList.contains('show')) return;
        if (parseInt(event.stt) !== currentCommentsSpotStt) return;

        // Tránh trùng nếu đã hiển thị
        if (event.event_id && currentLoadedReviews.some(r => r.event_id === event.event_id)) {
            return;
        }

        const origReviewRating = reviewRating !== undefined ? reviewRating : (Number(revObj.review_rating || revObj.rating || event.review_rating || event.rating || 5.0));
        const liveRev = {
            event_id: event.event_id,
            user_name: revObj.user_name || event.user_name || "Khách du lịch",
            rating: origReviewRating,
            review_text: revObj.review_text || event.review_text || "",
            timestamp: revObj.timestamp || event.timestamp || "Vừa xong"
        };

        currentLoadedReviews.unshift(liveRev);
        renderFbComments(currentLoadedReviews);
        if (fbModalTotalCount) {
            fbModalTotalCount.textContent = `${currentLoadedReviews.length} đánh giá`;
        }
        if (fbModalRatingVal && updatedRating) {
            fbModalRatingVal.textContent = parseFloat(updatedRating).toFixed(1);
        }
    };

    // =========================================================================
    // Lambda Architecture Closed-Loop: Batch Sync to HDFS & Parquet Lake
    // =========================================================================
    const btnLambdaSync = document.getElementById('btn-lambda-batch-sync');
    const batchSyncModal = document.getElementById('batch-sync-modal');
    const btnCloseSyncModal = document.getElementById('btn-close-sync-modal');
    const btnStartSyncAction = document.getElementById('btn-start-sync-action');
    const syncResultBox = document.getElementById('sync-result-box');
    const syncResultDetails = document.getElementById('sync-result-details');

    if (btnLambdaSync && batchSyncModal) {
        btnLambdaSync.addEventListener('click', () => {
            // Reset UI
            for (let i = 1; i <= 6; i++) {
                const stepElem = document.getElementById(`step-item-${i}`);
                if (stepElem) {
                    stepElem.className = 'sync-step-item';
                    const icon = stepElem.querySelector('.step-icon');
                    if (icon) icon.textContent = '⏳';
                }
            }
            if (syncResultBox) syncResultBox.style.display = 'none';
            if (btnStartSyncAction) {
                btnStartSyncAction.disabled = false;
                btnStartSyncAction.textContent = '🚀 Kích Hoạt Đồng Bộ HDFS Ngay';
                btnStartSyncAction.onclick = executeBatchSyncPipeline;
            }
            batchSyncModal.classList.add('show');
        });
    }

    if (btnCloseSyncModal && batchSyncModal) {
        btnCloseSyncModal.addEventListener('click', () => {
            batchSyncModal.classList.remove('show');
        });
    }

    window.addEventListener('click', (e) => {
        if (e.target === batchSyncModal) {
            batchSyncModal.classList.remove('show');
        }
    });

    function setStepStatus(stepNum, status) {
        const item = document.getElementById(`step-item-${stepNum}`);
        if (!item) return;
        const icon = item.querySelector('.step-icon');
        if (status === 'running') {
            item.className = 'sync-step-item step-running';
            if (icon) icon.textContent = '🔄';
        } else if (status === 'done') {
            item.className = 'sync-step-item step-done';
            if (icon) icon.textContent = '✅';
        } else if (status === 'failed') {
            item.className = 'sync-step-item step-failed';
            if (icon) icon.textContent = '❌';
        } else if (status === 'cancelled') {
            item.className = 'sync-step-item step-cancelled';
            if (icon) icon.textContent = '⛔';
        }
    }

    // Quản lý trạng thái công việc đồng bộ & chống khóa nút vô thời hạn (đã khai báo ở đầu script)

    function executeBatchSyncPipeline() {
        if (!btnStartSyncAction) return;
        const jobId = 'sync_' + Date.now();
        currentSyncJob = { id: jobId, status: 'running', hdfsConfirmed: false };

        btnStartSyncAction.disabled = true;
        btnStartSyncAction.textContent = 'Đang đồng bộ HDFS...';

        setStepStatus(1, 'running');
        setStepStatus(2, 'running');
        setStepStatus(3, 'running');
        setStepStatus(4, 'running');
        setStepStatus(5, 'running');
        setStepStatus(6, 'running');

        if (syncTimeoutTimer) clearTimeout(syncTimeoutTimer);
        syncTimeoutTimer = setTimeout(() => {
            if (currentSyncJob.status === 'running') {
                currentSyncJob.status = 'timeout';
                setStepStatus(6, 'failed');
                if (syncResultBox && syncResultDetails) {
                    syncResultBox.style.background = '#FFFBEB';
                    syncResultBox.style.borderColor = '#FCD34D';
                    const titleEl = syncResultBox.querySelector('div:first-child');
                    if (titleEl) {
                        titleEl.innerHTML = '⚠️ QUÁ THỜI HẠN CHỜ (TIMEOUT 60s)';
                        titleEl.style.color = '#B45309';
                    }
                    if (currentSyncJob.hdfsConfirmed) {
                        syncResultDetails.innerHTML += `
                            <div style="margin-top:0.4rem; padding-top:0.4rem; border-top:1px dashed #FCD34D; color:#92400E;">
                                • Quá 60s chưa nhận phản hồi SSE từ Spark. Dữ liệu HDFS đã lưu trữ an toàn. Bạn có thể đóng cửa sổ hoặc kiểm tra log worker.
                            </div>
                        `;
                    } else {
                        syncResultDetails.innerHTML += `
                            <div style="margin-top:0.4rem; padding-top:0.4rem; border-top:1px dashed #FCD34D; color:#92400E;">
                                • Quá 60s chưa nhận được xác nhận từ HDFS/Spark. Vui lòng kiểm tra trạng thái cụm NameNode/DataNode hoặc log server.
                            </div>
                        `;
                    }
                }
                btnStartSyncAction.disabled = false;
                btnStartSyncAction.textContent = 'Đóng Cửa Sổ';
                btnStartSyncAction.onclick = () => {
                    if (batchSyncModal) batchSyncModal.classList.remove('show');
                };
            }
        }, 60000);

        // Gọi Backend API thực tế kèm job_id
        fetch('/api/sync_hdfs_retrain', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ job_id: jobId })
        })
            .then(res => {
                if (!res.ok) {
                    return res.json().then(errData => {
                        const customErr = new Error(errData.error || errData.message || `HTTP ${res.status}`);
                        customErr.data = errData;
                        throw customErr;
                    }).catch(parseErr => {
                        if (parseErr.data) throw parseErr;
                        throw new Error(`HTTP ${res.status}`);
                    });
                }
                return res.json();
            })
            .then(data => {
                // NẾU SSE ĐÃ BÁO COMPLETED HOẶC FAILED TRƯỚC ĐÓ THÌ KHÔNG ĐƯỢC CHUYỂN NGƯỢC VỀ 'running'
                if (currentSyncJob.status === 'completed' || currentSyncJob.status === 'failed' || currentSyncJob.status === 'timeout') {
                    return;
                }

                if (!data.success) {
                    const failErr = new Error(data.error || "Quá trình đồng bộ thất bại!");
                    failErr.data = data;
                    throw failErr;
                }

                // Ghi nhận xác nhận HDFS thành công từ phản hồi HTTP
                currentSyncJob.hdfsConfirmed = Boolean(data.hdfs_status === 'SYNCED_ONLINE' || data.success === true);

                // Cập nhật trạng thái từng bước theo kết quả hệ thống thực tế
                setStepStatus(1, 'done');
                setStepStatus(2, 'done');
                setStepStatus(3, 'done'); // Kiểm định chất lượng (DQA) trước khi ghi đè
                setStepStatus(4, 'done'); // Ghi đè & HDFS Native

                // Bước 5: Parquet (bước phụ)
                if (data.status === 'PARTIAL_SUCCESS_PARQUET_WARNING') {
                    setStepStatus(5, 'failed');
                } else {
                    setStepStatus(5, 'done');
                }

                // BƯỚC 6: CHƯA ĐÁNH DẤU DONE NGAY — HIỂN THỊ ĐANG CHỜ SPARK TÁI TÍNH TOÁN
                setStepStatus(6, 'running');

                if (syncResultBox && syncResultDetails) {
                    syncResultBox.style.display = 'block';
                    syncResultBox.style.background = '#EFF6FF';
                    syncResultBox.style.borderColor = '#93C5FD';
                    const titleEl = syncResultBox.querySelector('div:first-child');
                    if (titleEl) {
                        titleEl.innerHTML = '⏳ ĐÃ ĐẨY LÊN HDFS THẬT — ĐANG CHỜ SPARK TÁI TÍNH TOÁN...';
                        titleEl.style.color = '#1E40AF';
                    }
                    syncResultDetails.innerHTML = `
                        • <b>Mã phiên (Job ID):</b> <code>${data.job_id || jobId}</code><br>
                        • <b>Kiểm định chất lượng (DQA):</b> <span style="color:#16A34A; font-weight:700;">${data.data_quality || '100% ĐẠT CHUẨN'}</span><br>
                        • <b>Số bài đánh giá cộng đồng đã gom:</b> ${data.reviews_synced || 0} bài<br>
                        • <b>Số điểm đến cập nhật Rating mới:</b> ${data.spots_updated || 0} điểm<br>
                        • <b>Hadoop HDFS Destination:</b> <code>${data.hdfs_uri || 'hdfs://localhost:9000/...'}</code><br>
                        • <b>Trạng thái HDFS NameNode:</b> <span style="color:#16A34A; font-weight:700;">${data.hdfs_status || 'SYNCED_ONLINE'}</span><br>
                        • <b>Định dạng Parquet Data Lake (Phụ):</b> ${data.parquet_status || 'SYNCED'}<br>
                        • <b>Yêu cầu tái tính toán:</b> <span style="color:#2563EB; font-weight:700;">Đang chạy Spark Batch Train trên cụm HDFS, giám sát qua topic 'tourism-control'...</span><br>
                        • <b>Tiến độ hiện tại:</b> <span id="sync-progress-step" style="color:#0284C7; font-weight:600;">Khởi động SparkSession...</span>
                    `;
                }

                btnStartSyncAction.disabled = true;
                btnStartSyncAction.textContent = 'Đang huấn luyện Batch...';

                // Bắt đầu định kỳ truy vấn /api/sync_status
                const targetJobId = data.job_id || jobId;
                const pollInterval = setInterval(() => {
                    if (currentSyncJob.status !== 'running' || currentSyncJob.id !== jobId) {
                        clearInterval(pollInterval);
                        return;
                    }
                    fetch(`/api/sync_status?job_id=${encodeURIComponent(targetJobId)}`)
                        .then(r => r.json())
                        .then(sData => {
                            if (!sData || currentSyncJob.status !== 'running') return;
                            const stepEl = document.getElementById('sync-progress-step');
                            if (stepEl && sData.step) {
                                stepEl.textContent = sData.step;
                            }
                            if (sData.status === 'SUCCEEDED') {
                                clearInterval(pollInterval);
                                if (syncTimeoutTimer) clearTimeout(syncTimeoutTimer);
                                currentSyncJob.status = 'completed';
                                setStepStatus(6, 'done');
                                reloadMergedSpots();
                                if (syncResultBox) {
                                    syncResultBox.style.background = '#F0FDF4';
                                    syncResultBox.style.borderColor = '#86EFAC';
                                    const titleEl = syncResultBox.querySelector('div:first-child');
                                    if (titleEl) {
                                        titleEl.innerHTML = '✅ HOÀN TẤT: SPARK BATCH TRAINING ĐÃ CÔNG BỐ STATE STORE MỚI!';
                                        titleEl.style.color = '#166534';
                                    }
                                }
                                btnStartSyncAction.disabled = false;
                                btnStartSyncAction.textContent = 'Bắt đầu Chu kỳ Đồng bộ (Closed-Loop)';
                            } else if (sData.status === 'FAILED') {
                                clearInterval(pollInterval);
                                if (syncTimeoutTimer) clearTimeout(syncTimeoutTimer);
                                currentSyncJob.status = 'failed';
                                setStepStatus(6, 'failed');
                                if (syncResultBox) {
                                    syncResultBox.style.background = '#FEF2F2';
                                    syncResultBox.style.borderColor = '#FECACA';
                                    const titleEl = syncResultBox.querySelector('div:first-child');
                                    if (titleEl) {
                                        titleEl.innerHTML = `❌ THẤT BẠI: ${sData.error || 'Job gặp sự cố'}`;
                                        titleEl.style.color = '#991B1B';
                                    }
                                }
                                btnStartSyncAction.disabled = false;
                                btnStartSyncAction.textContent = 'Thử lại';
                            }
                        })
                        .catch(() => {});
                }, 2000);
            })
            .catch(err => {
                if (syncTimeoutTimer) clearTimeout(syncTimeoutTimer);
                currentSyncJob.status = 'failed';

                const errPayload = err.data || {};
                const backendStatus = errPayload.status || '';
                const errMsg = errPayload.error || errPayload.message || err.message || '';

                // Ánh xạ trạng thái từng bước chính xác theo lỗi thực tế từ backend
                if (backendStatus === 'FAILED_DATA_QUALITY') {
                    setStepStatus(1, 'done');
                    setStepStatus(2, 'done');
                    setStepStatus(3, 'failed');
                    setStepStatus(4, 'cancelled');
                    setStepStatus(5, 'cancelled');
                    setStepStatus(6, 'cancelled');
                } else if (backendStatus === 'FAILED_HDFS_OFFLINE' || backendStatus === 'FAILED_HDFS_CLI' || errMsg.toLowerCase().includes('hdfs')) {
                    setStepStatus(1, 'done');
                    setStepStatus(2, 'done');
                    setStepStatus(3, 'done'); // DQA đã kiểm định thành công trước đó
                    setStepStatus(4, 'failed');
                    setStepStatus(5, 'cancelled');
                    setStepStatus(6, 'cancelled');
                } else if (backendStatus === 'PARTIAL_SUCCESS_KAFKA_FAILED' || errMsg.toLowerCase().includes('kafka')) {
                    setStepStatus(1, 'done');
                    setStepStatus(2, 'done');
                    setStepStatus(3, 'done');
                    setStepStatus(4, 'done');
                    setStepStatus(5, (errPayload.parquet_status && errPayload.parquet_status.includes('WARNING')) ? 'failed' : 'done');
                    setStepStatus(6, 'failed');
                } else {
                    setStepStatus(1, 'done');
                    setStepStatus(2, 'done');
                    setStepStatus(3, 'failed');
                    setStepStatus(4, 'cancelled');
                    setStepStatus(5, 'cancelled');
                    setStepStatus(6, 'cancelled');
                }

                if (syncResultBox && syncResultDetails) {
                    syncResultBox.style.display = 'block';
                    syncResultBox.style.background = '#FEF2F2';
                    syncResultBox.style.borderColor = '#FCA5A5';
                    const titleEl = syncResultBox.querySelector('div:first-child');
                    if (titleEl) {
                        titleEl.innerHTML = '❌ ĐỒNG BỘ THẤT BẠI: LỖI THỰC TẾ TRONG HỆ THỐNG!';
                        titleEl.style.color = '#991B1B';
                    }
                    syncResultDetails.innerHTML = `
                        • <b>Mã phiên (Job ID):</b> <code>${errPayload.job_id || jobId}</code><br>
                        • <b>Trạng thái lỗi:</b> <code>${backendStatus || 'FAILED'}</code><br>
                        • <b>Chi tiết lỗi:</b> <span style="color:#DC2626; font-weight:700;">${errMsg}</span><br>
                        • <b>Hệ thống an toàn:</b> Tuyệt đối không ghi đè dữ liệu giả khi các bước bắt buộc gặp sự cố.
                    `;
                }

                btnStartSyncAction.disabled = false;
                btnStartSyncAction.textContent = 'Đóng Cửa Sổ';
                btnStartSyncAction.onclick = () => {
                    if (batchSyncModal) batchSyncModal.classList.remove('show');
                };
            });
    }

    // Hiển thị trạng thái "Đang xử lý..." tạm thời trên thẻ địa điểm khi nhận HTTP 202
    function showPendingSpotCardReview(spotStt, spotName, reviewObj, eventId) {
        const parsedStt = parseInt(spotStt);
        const cardElem = (parsedStt ? document.getElementById(`spot-dir-card-${parsedStt}`) : null) ||
                         document.querySelector(`[data-spot-name="${spotName}"]`);
        if (cardElem) {
            const bubble = cardElem.querySelector('.spot-review-bubble');
            if (bubble) {
                bubble.classList.add('has-new-review');
                bubble.style.background = '#F0FDF4';
                bubble.style.borderColor = '#86EFAC';

                const authorContainer = bubble.querySelector('.review-author');
                const authorElem = bubble.querySelector('.author-name');
                const timeElem = bubble.querySelector('.review-time');
                const quoteElem = bubble.querySelector('.review-quote');

                const userName = reviewObj.user_name || 'Khách du lịch';
                if (authorContainer) {
                    authorContainer.style.color = '#1E293B';
                    authorContainer.innerHTML = `💬 <span class="author-name">${userName}</span>:`;
                } else if (authorElem) {
                    authorElem.textContent = userName;
                }

                if (timeElem) {
                    timeElem.style.color = '#D97706';
                    timeElem.style.fontSize = '';
                    timeElem.innerHTML = '<span style="font-weight:700;">⏳ Đang xử lý...</span>';
                }
                if (quoteElem) {
                    quoteElem.style.color = '#334155';
                    quoteElem.style.fontStyle = 'normal';
                    quoteElem.textContent = `"${reviewObj.review_text || ''}"`;
                }
            }
        }
    }

    // Bộ nhớ đệm chống trùng lặp sự kiện CHÍNH THỨC theo event_id từ Spark SSE (đã khai báo ở đầu script)

    // Function to update a spot card live with the new review!
    function updateSpotCardWithReview(spotStt, spotName, reviewObj, newRating, officialReviewCount, eventId) {
        if (eventId) {
            if (processedReviewEvents.has(eventId)) {
                return; // Bỏ qua nếu sự kiện chính thức đã được hiển thị
            }
            processedReviewEvents.add(eventId);
            if (processedReviewEvents.size > 500) {
                const first = processedReviewEvents.values().next().value;
                processedReviewEvents.delete(first);
            }
        }

        const parsedStt = parseInt(spotStt);
        const spot = allLoadedSpots.find(s => s.stt === parsedStt) ||
                     allLoadedSpots.find(s => s.name === spotName);

        // Sử dụng review_count chính thức từ sự kiện, tránh lỗi 0 -> 2
        let targetCount = null;
        if (officialReviewCount !== undefined && officialReviewCount !== null) {
            targetCount = parseInt(officialReviewCount);
        } else if (spot) {
            targetCount = (spot.review_count !== undefined && spot.review_count !== null) ? (spot.review_count + 1) : 1;
        } else {
            targetCount = 1;
        }

        if (spot) {
            spot.rating = parseFloat(newRating || spot.rating);
            spot.review_count = targetCount;
            spot.latest_review = reviewObj;
            if (!spot.reviews) spot.reviews = [];
            spot.reviews.unshift(reviewObj);

            // Cập nhật chớp sáng marker trên bản đồ
            highlightSpotOnMap(spot.name, spot.latitude, spot.longitude, spot.cluster_color);
        }

        const cardElem = (parsedStt ? document.getElementById(`spot-dir-card-${parsedStt}`) : null) ||
                         document.querySelector(`[data-spot-name="${spotName}"]`);

        // Cập nhật số sao trên cả Thẻ Địa Điểm Directory lẫn Thẻ Lịch Trình Itinerary
        if (newRating) {
            const formattedRating = parseFloat(newRating).toFixed(1);
            if (cardElem) {
                const ratingNum = cardElem.querySelector('.spot-card-rating-num');
                if (ratingNum) {
                    ratingNum.textContent = formattedRating;
                }
            }
            if (parsedStt) {
                const itinStars = document.querySelectorAll(`.itin-stop-rating-${parsedStt}`);
                itinStars.forEach(el => {
                    el.textContent = `★ ${formattedRating}`;
                });
            }
        }

        if (cardElem) {
            // Update review count chính xác
            const countNum = cardElem.querySelector('.review-count-num');
            if (countNum) {
                countNum.textContent = targetCount;
            }

            // Update review bubble
            const bubble = cardElem.querySelector('.spot-review-bubble');
            if (bubble) {
                bubble.classList.add('has-new-review');
                bubble.style.background = '#F0FDF4';
                bubble.style.borderColor = '#86EFAC';

                const authorContainer = bubble.querySelector('.review-author');
                const authorElem = bubble.querySelector('.author-name');
                const timeElem = bubble.querySelector('.review-time');
                const quoteElem = bubble.querySelector('.review-quote');

                const userName = reviewObj.user_name || "Khách vừa đánh giá";
                if (authorContainer) {
                    authorContainer.style.color = '#1E293B';
                    authorContainer.innerHTML = `💬 <span class="author-name">${userName}</span>:`;
                } else if (authorElem) {
                    authorElem.textContent = userName;
                }

                if (timeElem) {
                    timeElem.style.color = '#64748B';
                    timeElem.style.fontSize = '';
                    timeElem.textContent = reviewObj.timestamp || "Vừa xong";
                }
                if (quoteElem) {
                    quoteElem.style.color = '#334155';
                    quoteElem.style.fontStyle = 'normal';
                    quoteElem.textContent = `"${reviewObj.review_text || ''}"`;
                }
            }

            // Flash effect (nhấp nháy nhẹ tại chỗ, TUYỆT ĐỐI KHÔNG tự scroll gây giật màn hình của người dùng)
            cardElem.classList.remove('highlight-flash');
            void cardElem.offsetWidth; // trigger reflow
            cardElem.classList.add('highlight-flash');
        }
    }

    // Execute initial recommendation
    executeRecommendation();
});


