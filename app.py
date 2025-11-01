# -*- coding: utf-8 -*-
"""
[상우고] Arang's 파일 태그 검색기 (v.0.1.3, 정돈/주석/칩테두리강화판)
- 태그 칩(버튼) 렌더러 + 두꺼운 테두리
- 파일(0열)만 태그색 배경 적용
- 경로별 파스텔색/툴팁, 테이블 호버 색상 등 UI 개선
"""

import os
import sys
import sqlite3
import threading
import hashlib
import ctypes
import webbrowser  # ★ 링크 열기용
from ctypes import wintypes
from pathlib import Path
from datetime import datetime

from PySide6.QtCore import Qt, Signal, QTimer, QRectF
from PySide6.QtGui import QColor, QBrush, QShortcut, QKeySequence, QPen
from PySide6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QLineEdit, QPushButton,
    QFileDialog, QTableWidget, QTableWidgetItem, QListWidget, QListWidgetItem,
    QSplitter, QMessageBox, QLabel, QCheckBox, QAbstractItemView, QComboBox,
    QHeaderView, QProgressBar, QInputDialog, QMenu, QSizePolicy,
    QStyledItemDelegate, QStyleOptionViewItem, QStyle
)

# =========================[ 전역 상수 / 앱 타이틀 ]=========================
DB_PATH = "filetags.db"
APP_TITLE = "[상우고] Arang's 파일 태그 검색기(v.0.1.3) / 제작 : 독서하는 경호t"

# UI 치수
BTN_H = 26        # 버튼 높이
EDIT_H = 28       # 입력창 높이
SEARCH_W = 190    # 검색창 너비 (버튼 60 포함 총 250)
SEARCH_BTN_W = 60

# =========================[ 공통 유틸 ]======================================
def normalize_path(p: str) -> str:
    """경로 정규화: 슬래시/드라이브 표기 정리(D: -> D:\\)."""
    if not p:
        return p
    np = os.path.normpath(p)
    if len(np) == 2 and np[1] == ':':  # 'D:' -> 'D:\'
        np = np + os.sep
    return np

def format_size_explorer(n: int) -> str:
    """
    Windows 탐색기 유사 표기:
      <1MB  -> 정수 KB (천단위 콤마): '59KB'
      <1GB  -> 1자리 소수 MB: '1.2MB'
      그 이상 -> 1자리 소수 GB: '3.4GB'
    """
    if n < 1024:
        return f"{n}B"
    kb = n / 1024.0
    if n < 1024**2:
        return f"{int(round(kb)):,}KB"
    mb = kb / 1024.0
    if n < 1024**3:
        s = f"{mb:.1f}".rstrip("0").rstrip(".")
        return f"{s}MB"
    gb = mb / 1024.0
    s = f"{gb:.1f}".rstrip("0").rstrip(".")
    return f"{s}GB"

# =========================[ DB 연결/스키마 ]==================================
def get_conn():
    """SQLite 연결 핸들 반환(+FK ON). 사용 후 반드시 close()."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def init_db():
    """최초 실행 시 DB 테이블/인덱스 생성 + ord 필드 백필."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS files(
        id INTEGER PRIMARY KEY,
        path TEXT UNIQUE,
        size INTEGER,
        mtime REAL,
        hash TEXT
    );""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS tags(
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE,
        ord INTEGER
    );""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS file_tags(
        file_id INTEGER,
        tag_id INTEGER,
        UNIQUE(file_id, tag_id),
        FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE,
        FOREIGN KEY(tag_id) REFERENCES tags(id) ON DELETE CASCADE
    );""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS roots(
        path TEXT PRIMARY KEY,
        last_scanned REAL
    );""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS settings(
        key TEXT PRIMARY KEY,
        value TEXT
    );""")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_files_path ON files(path);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_tags_name ON tags(name);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_file_tags_file ON file_tags(file_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_files_mtime ON files(mtime);")
    conn.commit()

    # tags.ord 채우기(초기 설치 호환)
    cur.execute("SELECT COUNT(*) FROM tags WHERE ord IS NULL;")
    if cur.fetchone()[0]:
        cur.execute("SELECT id FROM tags WHERE ord IS NULL ORDER BY name;")
        rows = cur.fetchall()
        nxt = 1
        for (tid,) in rows:
            cur.execute("UPDATE tags SET ord=? WHERE id=?;", (nxt, tid)); nxt += 1
        conn.commit()
    conn.close()

def get_setting(key, default=None):
    """설정값 조회 (없으면 default)."""
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT value FROM settings WHERE key=?;", (key,))
    row = cur.fetchone(); conn.close()
    return row[0] if row else default

def set_setting(key, value):
    """설정값 저장/갱신(UPSERT)."""
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""INSERT INTO settings(key,value) VALUES(?,?)
                   ON CONFLICT(key) DO UPDATE SET value=excluded.value;""",
                (key, value))
    conn.commit(); conn.close()

# =========================[ DB 조작 - 파일/태그/루트 ]========================
def upsert_file(path: str):
    """파일 메타(경로/크기/mtime)를 files 테이블에 UPSERT."""
    p = Path(path)
    if not p.is_file(): return
    st = p.stat()
    full = normalize_path(str(p))
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""
      INSERT INTO files(path,size,mtime,hash)
      VALUES(?,?,?,NULL)
      ON CONFLICT(path) DO UPDATE SET size=excluded.size, mtime=excluded.mtime;
    """, (full, st.st_size, st.st_mtime))
    conn.commit(); conn.close()

def remove_missing_under(root: str):
    """루트 하위에서 실제로 없는 파일을 DB에서 정리."""
    root = normalize_path(root)
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT id, path FROM files WHERE path LIKE ?;", (f"{root}%",))
    rows = cur.fetchall(); removed = 0
    for fid, fpath in rows:
        if not Path(fpath).exists():
            cur.execute("DELETE FROM files WHERE id=?;", (fid,)); removed += 1
    conn.commit(); conn.close(); return removed

def ensure_tag(name: str):
    """태그명이 없으면 생성 후 id 반환, 있으면 해당 id 반환."""
    name = (name or "").strip()
    if not name: return None
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT COALESCE(MAX(ord),0)+1 FROM tags;")
    nxt = cur.fetchone()[0]
    cur.execute("INSERT OR IGNORE INTO tags(name,ord) VALUES(?,?);", (name, nxt))
    cur.execute("SELECT id FROM tags WHERE name=?;", (name,))
    row = cur.fetchone(); conn.commit(); conn.close()
    return row[0] if row else None

def delete_tags(tag_ids):
    """태그 id 리스트 삭제."""
    if not tag_ids: return
    conn = get_conn(); cur = conn.cursor()
    cur.executemany("DELETE FROM tags WHERE id=?;", [(tid,) for tid in tag_ids])
    conn.commit(); conn.close()

def list_tags():
    """(id, name, ord) 리스트를 순서대로 반환."""
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT id, name, ord FROM tags ORDER BY ord ASC, name ASC;")
    rows = cur.fetchall(); conn.close(); return rows

def list_file_tags(file_id: int):
    """특정 파일에 걸린 태그 (id, name) 리스트 반환."""
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""
      SELECT t.id, t.name
      FROM tags t JOIN file_tags ft ON ft.tag_id=t.id
      WHERE ft.file_id=? ORDER BY t.ord, t.name;""", (file_id,))
    rows = cur.fetchall(); conn.close(); return rows

def add_root(path: str):
    """색인 루트 경로 추가/업데이트."""
    path = normalize_path(path)
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""INSERT INTO roots(path,last_scanned) VALUES(?,strftime('%s','now'))
                   ON CONFLICT(path) DO UPDATE SET last_scanned=strftime('%s','now');""", (path,))
    conn.commit(); conn.close()

def list_roots():
    """등록된 루트 경로 리스트(정규화) 반환."""
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT path FROM roots ORDER BY path;")
    rows = [normalize_path(r[0]) for r in cur.fetchall()]
    conn.close(); return rows

def remove_root(path: str):
    """루트 경로 제거 + 하위 파일 레코드 삭제."""
    path = normalize_path(path)
    conn = get_conn(); cur = conn.cursor()
    cur.execute("DELETE FROM roots WHERE path=?;", (path,))
    cur.execute("DELETE FROM files WHERE path LIKE ?;", (f"{path}%",))
    conn.commit(); conn.close()

# =========================[ 조회용 쿼리 ]=====================================
def count_files_under(root: str) -> int:
    """해당 루트 하위 files 개수를 LIKE로 카운트."""
    root = normalize_path(root)
    esc = root.replace("\\", "\\\\").replace("%", r"\%").replace("_", r"\_")
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM files WHERE path LIKE ? ESCAPE '\\';", (f"{esc}%",))
    n = cur.fetchone()[0]
    conn.close(); return n

def build_query(tag_ids, search_text, only_tagged):
    """파일 목록 조회용 동적 JOIN/WHERE 구성."""
    params, where, joins = [], [], []
    if search_text:
        where.append("f.path LIKE ?"); params.append(f"%{search_text}%")
    if only_tagged:
        where.append("EXISTS (SELECT 1 FROM file_tags x WHERE x.file_id=f.id)")
    if tag_ids:
        for i, tid in enumerate(tag_ids):
            alias = f"ft{i}"
            joins.append(f"JOIN file_tags {alias} ON {alias}.file_id=f.id AND {alias}.tag_id=?")
            params.insert(0, tid)
    return " ".join(joins), (" WHERE " + " AND ".join(where)) if where else "", params

def list_files(search_text, tag_ids, only_tagged, root_prefix=None):
    """필터(검색어/태그/루트)로 files 목록 반환."""
    join_sql, where_sql, params = build_query(tag_ids or [], search_text, only_tagged)
    if root_prefix:
        root_prefix = normalize_path(root_prefix)
        where_sql += (" AND " if where_sql else " WHERE ") + "f.path LIKE ?"
        params.append(f"{root_prefix}%")
    sql_order = " ORDER BY f.path ASC "
    sql = f"""
    SELECT f.id, f.path, f.size, f.mtime,
           (SELECT GROUP_CONCAT(t.name, ', ')
              FROM tags t JOIN file_tags ft ON ft.tag_id=t.id
             WHERE ft.file_id=f.id) AS tags
      FROM files f
      {join_sql}
      {where_sql}
      {sql_order};"""
    conn = get_conn(); cur = conn.cursor()
    cur.execute(sql, params); rows = cur.fetchall(); conn.close()
    return rows

def count_all_files():
    """전체 files 개수 반환."""
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM files;")
    n = cur.fetchone()[0]
    conn.close(); return n

def count_files_by_tag() -> dict[int, int]:
    """태그별 파일 개수 딕셔너리 {tag_id: count}."""
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""
        SELECT t.id, COALESCE(COUNT(ft.file_id),0) AS cnt
          FROM tags t
          LEFT JOIN file_tags ft ON ft.tag_id=t.id
         GROUP BY t.id
         ORDER BY t.ord, t.name;
    """)
    rows = cur.fetchall(); conn.close()
    return {tid: cnt for tid, cnt in rows}

# =========================[ 파일 시스템 유틸 ]================================
def count_files_on_disk(root: str) -> int:
    """디스크에서 루트 하위 파일 개수 빠른 카운트(os.walk)."""
    root = normalize_path(root)
    c = 0
    for _dp, _dirs, files in os.walk(root):
        c += len(files)
    return c

def walk_count_and_max_mtime(root: str) -> tuple[int, float]:
    """디스크에서 (파일개수, 최대 mtime) 계산."""
    root = normalize_path(root)
    total = 0
    max_m = 0.0
    for dp, _dirs, files in os.walk(root):
        for fn in files:
            total += 1
            try:
                mt = os.path.getmtime(os.path.join(dp, fn))
                if mt > max_m:
                    max_m = mt
            except Exception:
                pass
    return total, max_m

def db_count_and_max_mtime_under(root: str) -> tuple[int, float]:
    """DB에서 (파일개수, 최대 mtime) 조회."""
    root = normalize_path(root)
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT COUNT(*), COALESCE(MAX(mtime),0) FROM files WHERE path LIKE ?;", (f"{root}%",))
    n, mx = cur.fetchone()
    conn.close()
    return int(n or 0), float(mx or 0.0)

def show_in_explorer(path: str):
    """OS 탐색기에서 파일/폴더 열기."""
    if sys.platform.startswith("win"):
        os.system(f'explorer /select,"{path}"')
    elif sys.platform == "darwin":
        os.system(f'open -R "{path}"')
    else:
        os.system(f'xdg-open "{os.path.dirname(path)}"')

def recycle_delete(path: str) -> bool:
    """Windows: 휴지통으로 / 그외 OS: 영구 삭제. 성공시 True."""
    try:
        if sys.platform.startswith("win"):
            class SHFILEOPSTRUCT(ctypes.Structure):
                _fields_ = [
                    ('hwnd', wintypes.HWND), ('wFunc', wintypes.UINT),
                    ('pFrom', wintypes.LPCWSTR), ('pTo', wintypes.LPCWSTR),
                    ('fFlags', ctypes.c_uint), ('fAnyOperationsAborted', wintypes.BOOL),
                    ('hNameMappings', ctypes.c_void_p), ('lpszProgressTitle', wintypes.LPCWSTR)
                ]
            FO_DELETE = 0x0003
            FOF_ALLOWUNDO = 0x0040
            FOF_NOCONFIRMATION = 0x0010
            FOF_SILENT = 0x0004
            shell = ctypes.windll.shell32
            pFrom = normalize_path(path) + '\0\0'
            op = SHFILEOPSTRUCT(0, FO_DELETE, pFrom, None,
                                FOF_ALLOWUNDO | FOF_NOCONFIRMATION | FOF_SILENT,
                                False, None, None)
            res = shell.SHFileOperationW(ctypes.byref(op))
            return res == 0 and not op.fAnyOperationsAborted
        else:
            os.remove(path)
            return True
    except Exception:
        return False

def db_delete_file_by_path(path: str):
    """files 테이블에서 해당 경로 레코드 삭제."""
    conn = get_conn(); cur = conn.cursor()
    cur.execute("DELETE FROM files WHERE path=?;", (normalize_path(path),))
    conn.commit(); conn.close()

def db_rename_file_path(old_path: str, new_path: str):
    """files 테이블의 경로 필드 갱신."""
    conn = get_conn(); cur = conn.cursor()
    cur.execute("UPDATE files SET path=? WHERE path=?;",
                (normalize_path(new_path), normalize_path(old_path)))
    conn.commit(); conn.close()

# =========================[ 색상/테마 유틸 ]=================================
PALETTE = [
    QColor(255,204,204), QColor(255,229,204), QColor(255,255,204), QColor(229,255,204),
    QColor(204,255,204), QColor(204,255,229), QColor(204,255,255), QColor(204,229,255),
    QColor(204,204,255), QColor(229,204,255), QColor(255,204,255), QColor(255,204,229),
    QColor(255,240,200), QColor(220,245,255), QColor(220,255,240), QColor(245,220,255)
]

def color_for_tag(tid: int, name: str) -> QColor:
    """태그 id/이름을 안정적인 파스텔색으로 매핑."""
    if tid is not None: return PALETTE[tid % len(PALETTE)]
    h = int(hashlib.sha1((name or '').encode()).hexdigest()[:2], 16)
    return PALETTE[h % len(PALETTE)]

def color_for_root_path(path: str) -> QColor:
    """루트 경로별 파스텔색(해시 기반)."""
    h = int(hashlib.sha1((normalize_path(path) or '').encode()).hexdigest()[:2], 16)
    return PALETTE[h % len(PALETTE)]

# =========================[ 태그 칩 렌더러 ]=================================
class TagChipsDelegate(QStyledItemDelegate):
    """태그 문자열(쉼표 구분)을 칩(버튼)처럼 그려주는 델리게이트 (4열 전용)."""

    def __init__(self, color_resolver, parent=None):
        super().__init__(parent)
        self._color_resolver = color_resolver

    def _chip_color(self, name: str) -> QColor:
        try:
            col = self._color_resolver(name)
            if isinstance(col, QColor):
                return col
        except Exception:
            pass
        return color_for_tag(None, name)

    def paint(self, painter, option, index):
        if index.column() != 3:
            return super().paint(painter, option, index)

        text = index.data(Qt.DisplayRole) or ""
        tags = [t.strip() for t in text.split(",") if t.strip()]

        opt = QStyleOptionViewItem(option)
        self.initStyleOption(opt, index)
        opt.text = ""
        w = opt.widget or None
        style = w.style() if w else QApplication.style()
        style.drawControl(QStyle.CE_ItemViewItem, opt, painter, w)

        if not tags:
            return

        painter.save()
        fm = opt.fontMetrics
        x = opt.rect.x() + 6
        y_center = opt.rect.y() + opt.rect.height() // 2
        pad_h, pad_v = 8, 4
        spacing = 6
        max_x = opt.rect.right() - 6
        hidden = 0

        pen = painter.pen()
        pen.setWidth(2)
        painter.setPen(pen)

        for t in tags:
            chip_w = fm.horizontalAdvance(t) + pad_h * 2
            chip_h = fm.height() + pad_v * 2
            if x + chip_w > max_x:
                hidden += 1
                break
            rect = QRectF(x, y_center - chip_h / 2, chip_w, chip_h)
            bg = self._chip_color(t)
            painter.setBrush(bg)
            painter.drawRoundedRect(rect, 10, 10)
            painter.drawText(rect, Qt.AlignCenter, t)
            x += chip_w + spacing

        if hidden > 0:
            more = f"+{hidden}"
            chip_w = fm.horizontalAdvance(more) + pad_h * 2
            chip_h = fm.height() + pad_v * 2
            if x + chip_w <= max_x:
                rect = QRectF(x, y_center - chip_h / 2, chip_w, chip_h)
                painter.setBrush(QColor(229, 231, 235))
                painter.drawRoundedRect(rect, 10, 10)
                painter.drawText(rect, Qt.AlignCenter, more)

        painter.restore()

# =========================[ 메인 UI ]========================================
class MainUI(QWidget):
    """메인 애플리케이션 위젯."""
    progress_tick = Signal(int, int)
    scan_started  = Signal(int)
    scan_finished = Signal()

    def __init__(self):
        super().__init__()
        self.setWindowTitle(APP_TITLE)
        self.resize(1320, 800)

        # 상태 필드
        self.root_dir = None
        self.root_filter = None
        self.selected_tag_ids = set()
        self.scan_running = False
        self.tag_color_by_name = {}

        # ---------- 상단 영역 ----------
        top = QVBoxLayout()
        title = QLabel(APP_TITLE)
        title.setAlignment(Qt.AlignCenter)
        title.setStyleSheet("font-size: 20pt; font-weight: 900; margin: 4px 0 8px 0;")
        top.addWidget(title)

        row1 = QHBoxLayout()
        self.root_path_edit = QLineEdit()
        self.root_path_edit.setReadOnly(True)
        self.root_path_edit.setPlaceholderText("루트를 선택하세요…")
        self.root_path_edit.setFixedHeight(EDIT_H)
        self.root_path_edit.setStyleSheet("font-size:12pt;")

        self.btn_pick = QPushButton("루트 선택")
        self.btn_index = QPushButton("선택된 루트 색인하기")
        for b in (self.btn_pick, self.btn_index):
            b.setFixedHeight(BTN_H); b.setStyleSheet("font-size:12pt; font-weight:700;")

        self.count_lbl = QLabel("결과: 0건")
        self.count_lbl.setStyleSheet("font-size:12pt; font-weight:800; padding-left:8px;")

        self.progress = QProgressBar()
        self.progress.setVisible(False)
        self.progress.setTextVisible(False)
        self.progress.setFixedHeight(10)

        self.search = QLineEdit(); self.search.setPlaceholderText("파일명 검색…")
        self.search.setObjectName("searchEdit")
        self.search.setFixedHeight(EDIT_H)
        self.search.setFixedWidth(SEARCH_W)
        self.search.setStyleSheet("font-size:12pt;")
        self.btn_search = QPushButton("검색")
        self.btn_search.setFixedHeight(BTN_H)
        self.btn_search.setFixedWidth(SEARCH_BTN_W)
        self.btn_search.setStyleSheet("font-size:12pt; font-weight:700;")

        row1.addWidget(self.root_path_edit, 1)
        row1.addWidget(self.btn_pick)
        row1.addWidget(self.btn_index)
        row1.addSpacing(8)
        row1.addWidget(self.count_lbl)
        row1.addSpacing(8)
        row1.addWidget(self.progress, 1)
        row1.addStretch(1)
        row1.addWidget(self.search)
        row1.addWidget(self.btn_search)
        top.addLayout(row1)

        # ---------- 우측 패널(루트/선택파일 태그) ----------
        right_box = QVBoxLayout()
        right_box.setContentsMargins(6, 6, 6, 6)
        right_box.setSpacing(6)

        self.btn_rescan_all = QPushButton("색인 전체 재스캔")
        self.btn_rescan_all.setObjectName("primaryBtn")
        self.btn_rescan_all.setFixedHeight(BTN_H)
        right_box.addWidget(self.btn_rescan_all)

        right_box.addWidget(QLabel("--[색인된 경로]---"))

        self.root_list = QListWidget()
        self.root_list.setFixedHeight(230)
        self.root_list.setSelectionMode(QAbstractItemView.SingleSelection)
        right_box.addWidget(self.root_list)

        self.btn_rescan_root = QPushButton("경로 재스캔")
        self.btn_rescan_root.setFixedHeight(BTN_H)
        self.btn_remove_root = QPushButton("제거")
        self.btn_remove_root.setFixedHeight(BTN_H)

        row_rescan_remove = QHBoxLayout()
        for b in (self.btn_rescan_root, self.btn_remove_root):
            b.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        row_rescan_remove.setSpacing(6)
        row_rescan_remove.addWidget(self.btn_rescan_root)
        row_rescan_remove.addWidget(self.btn_remove_root)
        right_box.addLayout(row_rescan_remove)

        # ★ 바로가기 줄 (재스캔/제거 아래)
        row_links = QHBoxLayout()
        self.btn_school_neis = QPushButton("학교NEIS")
        self.btn_external_evpn = QPushButton("외부EVPN")
        for b in (self.btn_school_neis, self.btn_external_evpn):
            b.setFixedHeight(BTN_H)
            b.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        # 개별 색상(빨강/초록, 흰 글자)
        self.btn_school_neis.setStyleSheet(
            "background:#DC2626; color:#FFFFFF; border:2px solid #000000; "
            "border-radius:10px; font-weight:900;"
        )
        self.btn_external_evpn.setStyleSheet(
            "background:#16A34A; color:#FFFFFF; border:2px solid #000000; "
            "border-radius:10px; font-weight:900;"
        )
        row_links.setSpacing(6)
        row_links.addWidget(self.btn_school_neis)
        row_links.addWidget(self.btn_external_evpn)
        right_box.addLayout(row_links)

        right_box.addSpacing(8)
        right_box.addWidget(QLabel("--[선택 파일의 태그]---"))
        self.sel_tags = QListWidget()
        self.sel_tags.setSelectionMode(QAbstractItemView.MultiSelection)
        right_box.addWidget(self.sel_tags, 1)
        self.combo_tag = QComboBox(); self.combo_tag.setEditable(True)
        self.combo_tag.setFixedHeight(EDIT_H)
        self.btn_assign = QPushButton("태그 붙이기"); self.btn_assign.setFixedHeight(BTN_H)
        self.btn_untag = QPushButton("떼기"); self.btn_untag.setFixedHeight(BTN_H)
        self.btn_assign.setObjectName("primaryBtn")
        right_box.addWidget(self.combo_tag)

        row_assign = QHBoxLayout()
        for b in (self.btn_assign, self.btn_untag):
            b.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        row_assign.setSpacing(6)
        row_assign.addWidget(self.btn_assign)
        row_assign.addWidget(self.btn_untag)
        right_box.addLayout(row_assign)

        right = QWidget(); right.setLayout(right_box)
        right.setMinimumWidth(190)

        # ---------- 좌측 패널(태그 목록) ----------
        left_box = QVBoxLayout()
        self.chk_only_tagged = QCheckBox("(체크)태그 파일 보기")
        left_box.addWidget(self.chk_only_tagged)
        left_box.addWidget(QLabel("---[태그]---"))

        self.tag_list = QListWidget()
        self.tag_list.setSelectionMode(QAbstractItemView.ExtendedSelection)
        left_box.addWidget(self.tag_list, 1)

        tag_btn_row1 = QHBoxLayout()
        self.btn_tag_up = QPushButton("▲ 위로"); self.btn_tag_up.setFixedHeight(BTN_H)
        self.btn_tag_down = QPushButton("▼ 아래로"); self.btn_tag_down.setFixedHeight(BTN_H)
        tag_btn_row1.addWidget(self.btn_tag_up)
        tag_btn_row1.addWidget(self.btn_tag_down)
        left_box.addLayout(tag_btn_row1)

        tag_btn_row2 = QHBoxLayout()
        self.btn_rename_tag = QPushButton("태그 이름 변경"); self.btn_rename_tag.setFixedHeight(BTN_H)
        tag_btn_row2.addWidget(self.btn_rename_tag)
        left_box.addLayout(tag_btn_row2)

        self.new_tag = QLineEdit(); self.new_tag.setPlaceholderText("새 태그 입력 후 추가")
        self.new_tag.setFixedHeight(EDIT_H)
        self.btn_add_tag = QPushButton("태그 추가"); self.btn_add_tag.setFixedHeight(BTN_H)
        self.btn_del_tag = QPushButton("태그 삭제"); self.btn_del_tag.setFixedHeight(BTN_H)
        self.btn_add_tag.setObjectName("primaryBtn")

        left_box.addWidget(self.new_tag)

        row_add_del = QHBoxLayout()
        for b in (self.btn_add_tag, self.btn_del_tag):
            b.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        row_add_del.setSpacing(6)
        row_add_del.addWidget(self.btn_add_tag)
        row_add_del.addWidget(self.btn_del_tag)
        left_box.addLayout(row_add_del)

        left = QWidget(); left.setLayout(left_box)
        left.setMinimumWidth(190)

        # ---------- 중앙(파일 테이블) ----------
        self.table = QTableWidget(0, 6)
        self.table.setObjectName("fileTable")
        self.table.setHorizontalHeaderLabels(["파일", "크기", "수정시각", "태그", "위치", "ID"])
        self.table.setColumnHidden(5, True)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.table.setAlternatingRowColors(True)
        self.table.setSortingEnabled(True)
        self.table.setShowGrid(True)
        self.table.setColumnWidth(0, 400)
        self.table.setColumnWidth(1, 80)
        self.table.setColumnWidth(2, 140)
        self.table.setColumnWidth(3, 240)
        self.table.setColumnWidth(4, 420)
        self.table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self.on_table_context_menu)
        self.table.setMouseTracking(True)

        # 태그 칩 렌더러(4번째 열)
        self.tag_delegate = TagChipsDelegate(
            lambda name: self.tag_color_by_name.get(name, color_for_tag(None, name)),
            self.table
        )
        self.table.setItemDelegateForColumn(3, self.tag_delegate)

        # 헤더 objectName 부여(스타일 구분)
        hh = self.table.horizontalHeader()
        vh = self.table.verticalHeader()
        hh.setObjectName("fileTableH")
        vh.setObjectName("fileTableV")

        # 스크롤바 항상 표시
        self.table.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        self.table.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        self.tag_list.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        self.root_list.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOn)
        self.sel_tags.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOn)

        splitter = QSplitter()
        splitter.addWidget(left); splitter.addWidget(self.table); splitter.addWidget(right)
        splitter.setSizes([200, 960, 200])
        splitter.setStretchFactor(1, 1)
        splitter.setCollapsible(0, False)
        splitter.setCollapsible(2, False)

        main = QVBoxLayout()
        main.addLayout(top)
        main.addWidget(splitter, 1)
        self.setLayout(main)

        # 표 높이/주요 버튼 스타일
        self.btn_index.setObjectName("primaryBtn")
        self.btn_rescan_root.setObjectName("primaryBtn")
        self.btn_assign.setObjectName("primaryBtn")
        self.table.verticalHeader().setDefaultSectionSize(29)
        self.table.horizontalHeader().setFixedHeight(31)

        self.apply_styles()  # 스타일 시트 적용

        # ---------- 시그널 바인딩 ----------
        self.btn_pick.clicked.connect(self.pick_root)
        self.btn_index.clicked.connect(self.index_selected_root)
        self.btn_rescan_root.clicked.connect(self.rescan_selected_root)
        self.btn_remove_root.clicked.connect(self.remove_selected_root)
        self.btn_add_tag.clicked.connect(self.add_tag)
        self.btn_del_tag.clicked.connect(self.delete_selected_tags)
        self.btn_tag_up.clicked.connect(lambda: self.move_tag(-1))
        self.btn_tag_down.clicked.connect(lambda: self.move_tag(+1))
        self.btn_rename_tag.clicked.connect(self.rename_selected_tag)
        self.tag_list.itemDoubleClicked.connect(self.rename_tag_inline)

        self.btn_search.clicked.connect(self.refresh_files_and_save)
        self.search.returnPressed.connect(self.refresh_files_and_save)
        self.chk_only_tagged.stateChanged.connect(self.on_only_tagged_toggled)
        self.tag_list.itemClicked.connect(self.on_tag_clicked)
        self.root_list.itemClicked.connect(self.on_root_clicked)
        self.table.itemSelectionChanged.connect(self.refresh_selected_file_tags)
        self.table.itemDoubleClicked.connect(self.open_file)
        self.btn_assign.clicked.connect(self.assign_tag_to_selected)
        self.btn_untag.clicked.connect(self.untag_selected_from_selected_files)

        self.btn_rescan_all.clicked.connect(self.rescan_all_roots)

        # 바로가기 버튼
        self.btn_school_neis.clicked.connect(lambda: self._open_url("https://goe.eduptl.kr/bpm_lgn_lg00_001.do"))  # ← URL만 바꿔 쓰면 됨
        self.btn_external_evpn.clicked.connect(lambda: self._open_url("https://evpn.goe.go.kr/custom/index.html"))  # 예시

        # 진행/완료 시그널
        self.scan_started.connect(self._on_scan_started)
        self.progress_tick.connect(self._on_progress_tick)
        self.scan_finished.connect(self._on_scan_finished)

        # ---------- 상태 복원 & 초기 로드 ----------
        self.root_dir = get_setting("last_root", "") or None
        if self.root_dir:
            self.root_path_edit.setText(self.root_dir)
        self.search.setText(get_setting("last_search", ""))
        self.chk_only_tagged.setChecked(get_setting("last_only_tagged", "0") == "1")
        self.update_checkbox_style()

        self.refresh_roots_panel()
        self.refresh_tags()
        self.refresh_files()

        # F5 즉시 리프레시
        self.short_refresh = QShortcut(QKeySequence("F5"), self)
        self.short_refresh.activated.connect(self.refresh_all_counts)

        # 시작 시 자동 전체 재스캔(루트 있으면)
        if list_roots():
            QTimer.singleShot(0, self.rescan_all_roots)
        else:
            QTimer.singleShot(0, self.pick_root)

    # --------------------- 스타일 시트 ---------------------
    def apply_styles(self):
        self.setStyleSheet(f"""
        QWidget {{
            background: #F3F4F6;
            color: #111827;
            font-size: 12pt;
            font-weight: 800;
        }}
        QLineEdit, QComboBox, QListWidget, QTableWidget {{
            background: #FFFFFF;
            color: #111827;
            border: 1px solid #000000;
            border-radius: 10px;
            font-weight: 800;
        }}
        QLineEdit:focus, QComboBox:focus {{
            border: 2px solid #000000;
            background: #FFFFFF;
        }}
        #fileTable {{ font-size: 10pt; border-radius: 10px; gridline-color: #FFFFFF; }}
        QHeaderView::section {{
            background: #111827;
            color: #ffffff;
            padding-top: 2px; padding-bottom: 2px; padding-left: 8px; padding-right: 8px;
            border: 0;
            font-weight: 900;
            font-size: 12pt;
            border-right: 1px solid #FFFFFF10;
        }}
        QHeaderView::section:last {{ border-right: 0; }}

        QTableWidget {{ alternate-background-color: #F7F7F8; }}
        QTableWidget::item:selected, QListWidget::item:selected {{
            background: #FDE68A;
            color: #111111;
        }}

        QHeaderView#fileTableH::section {{
            background: #111827; color: #ffffff;
            padding-top: 2px; padding-bottom: 2px; padding-left: 8px; padding-right: 8px;
            border: 0;
            border-right: 2px solid #F59E0B;
        }}
        QHeaderView#fileTableH::section:last {{ border-right: 0; }}
        QHeaderView#fileTableV::section {{
            background: #111827; color: #ffffff;
            padding-top: 2px; padding-bottom: 2px; padding-left: 6px; padding-right: 6px;
            border: 0;
            border-bottom: 1px solid #F59E0B;
        }}
        QTableCornerButton::section {{
            background: #111827; border: 0;
            border-right: 2px solid #F59E0B;
            border-bottom: 1px solid #F59E0B;
        }}

        QPushButton {{
            background: #E5E7EB;
            border: 2px solid #000000;
            padding: 2px 8px;
            border-radius: 10px;
            font-size: 12pt;
            font-weight: 800;
        }}
        QPushButton:hover {{ background: #D1D5DB; }}
        QPushButton:disabled {{ background: #E5E7EB; color: #9CA3AF; border-color: #000000; }}

        QPushButton#primaryBtn {{
            background: #2563EB;
            color: #FFFFFF;
            border: 2px solid #000000;
            padding: 2px 8px;
        }}
        QPushButton#primaryBtn:hover {{
            background: #1D4ED8;
            border: 2px solid #000000;
        }}

        QCheckBox {{ font-size: 12pt; font-weight: 800; }}
        QCheckBox::indicator {{
            width: 18px; height: 18px;
            border: 2px solid #000000; background: #FFFFFF; border-radius: 4px;
        }}
        QCheckBox::indicator:checked {{
            background: #DC2626; border: 2px solid #000000;
        }}
        QCheckBox::indicator:unchecked:hover {{ background: #FEE2E2; }}

        QProgressBar {{ border: 2px solid #000000; border-radius: 6px; height: 10px; text-align: center; }}
        QProgressBar::chunk {{ background: #2563EB; border-radius: 6px; }}

        QScrollBar:vertical {{ background: #202124; width: 16px; margin: 0px; }}
        QScrollBar::handle:vertical {{ background: #3D3D3D; border-radius: 6px; min-height: 32px; }}
        QScrollBar::handle:vertical:hover {{ background: #505050; }}
        QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{ height: 0; }}

        QScrollBar:horizontal {{ background: #202124; height: 16px; margin: 0px; }}
        QScrollBar::handle:horizontal {{ background: #3D3D3D; border-radius: 6px; min-width: 32px; }}
        QScrollBar::handle:horizontal:hover {{ background: #505050; }}
        QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {{ width: 0; }}

        /* 마우스 호버 시 빨간 배경 + 흰 글자 */
        QTableWidget#fileTable::item:hover {{ background: #DC2626; color: #FFFFFF; }}
        QTableWidget#fileTable::item:selected:hover {{ background: #DC2626; color: #FFFFFF; }}
        """)

    # --------------------- 진행 표시 슬롯 ---------------------
    def _on_scan_started(self, total: int):
        self.count_lbl.setText(f"색인 중… (0 / {total:,})")
        self.progress.setVisible(True)
        self.progress.setMaximum(max(total, 1))
        self.progress.setValue(0)

    def _on_progress_tick(self, processed: int, total: int):
        self.progress.setValue(processed)
        self.count_lbl.setText(f"색인 중… ({processed:,} / {total:,})")

    def _on_scan_finished(self):
        self.scan_running = False
        self.progress.setVisible(False)
        self.root_path_edit.clear()
        self.refresh_all_counts()
        self.search.setFocus()

    # --------------------- 패널 리프레시 ---------------------
    def refresh_roots_panel(self):
        self.root_list.clear()
        all_item = QListWidgetItem("전체 경로 보기")
        all_item.setData(Qt.UserRole, None)
        all_item.setToolTip("등록된 모든 경로 합산 보기")
        self.root_list.addItem(all_item)

        uniq = sorted({normalize_path(r) for r in list_roots()}, key=str.lower)
        for root in uniq:
            cnt = count_files_under(root)
            it = QListWidgetItem(f"{root} ({cnt:,})")
            it.setData(Qt.UserRole, root)
            it.setBackground(QBrush(color_for_root_path(root)))
            it.setToolTip(root)
            self.root_list.addItem(it)

    def save_state(self):
        set_setting("last_search", self.search.text().strip())
        set_setting("last_only_tagged", "1" if self.chk_only_tagged.isChecked() else "0")

    def refresh_all_counts(self):
        cur_item = self.root_list.currentItem()
        cur_path = cur_item.data(Qt.UserRole) if cur_item else None
        vpos = self.root_list.verticalScrollBar().value()

        self.refresh_roots_panel()

        if cur_path is None:
            self.root_list.setCurrentRow(0)
        else:
            for i in range(self.root_list.count()):
                if self.root_list.item(i).data(Qt.UserRole) == cur_path:
                    self.root_list.setCurrentRow(i); break
        self.root_list.verticalScrollBar().setValue(vpos)

        self.refresh_tags()
        self.refresh_files()
        self.count_lbl.setText(f"결과: {self.table.rowCount():,}건")

    def refresh_tags(self):
        self.tag_list.clear()
        total_cnt = count_all_files()

        self.tag_color_by_name = {}

        all_item = QListWidgetItem(f"전체 파일 ({total_cnt:,})")
        all_item.setData(Qt.UserRole, None)
        all_item.setBackground(QBrush(QColor(235, 235, 235)))
        self.tag_list.addItem(all_item)

        by_tag = count_files_by_tag()
        self.combo_tag.clear()
        for tid, name, _ord in list_tags():
            cnt = by_tag.get(tid, 0)
            it = QListWidgetItem(f"{name} ({cnt:,})")
            it.setData(Qt.UserRole, tid)
            col = color_for_tag(tid, name)
            it.setBackground(QBrush(col))
            self.tag_list.addItem(it)

            self.combo_tag.addItem(name, userData=tid)
            idx = self.combo_tag.count() - 1
            self.combo_tag.setItemData(idx, QBrush(col), Qt.BackgroundRole)

            self.tag_color_by_name[name] = col

        self.tag_list.setCurrentRow(0)

    # --------------------- 태그 조작 ---------------------
    def on_tag_clicked(self, item: QListWidgetItem):
        tid = item.data(Qt.UserRole)
        self.selected_tag_ids = set() if tid is None else {tid}
        self.refresh_files()

    def add_tag(self):
        name = self.new_tag.text().strip()
        if not name: return
        ensure_tag(name); self.new_tag.clear()
        self.refresh_tags()

    def delete_selected_tags(self):
        items = [it for it in self.tag_list.selectedItems() if it.data(Qt.UserRole) is not None]
        if not items:
            QMessageBox.information(self, "안내", "삭제할 태그를 선택하세요. (\"전체 파일\" 제외)")
            return

        names = [it.text().rsplit(" (", 1)[0] for it in items]
        preview = ", ".join(names[:10]) + ("…" if len(names) > 10 else "")
        if QMessageBox.question(self, "태그 삭제",
                                f"{len(items)}개 태그를 삭제할까요?\n{preview}") \
                != QMessageBox.StandardButton.Yes:
            return

        tids = [it.data(Qt.UserRole) for it in items]
        delete_tags(tids)
        self.refresh_all_counts()

    def move_tag(self, delta: int):
        cur = self.tag_list.currentItem()
        if not cur: return
        tid = cur.data(Qt.UserRole)
        if tid is None: return
        conn = get_conn(); c = conn.cursor()
        c.execute("SELECT ord FROM tags WHERE id=?;", (tid,))
        row = c.fetchone()
        if not row: conn.close(); return
        cur_ord = row[0]
        if delta < 0:
            c.execute("SELECT id, ord FROM tags WHERE ord<? ORDER BY ord DESC LIMIT 1;", (cur_ord,))
        else:
            c.execute("SELECT id, ord FROM tags WHERE ord>? ORDER BY ord ASC LIMIT 1;", (cur_ord,))
        nb = c.fetchone()
        if not nb: conn.close(); return
        nb_id, nb_ord = nb
        c.execute("UPDATE tags SET ord=? WHERE id=?;", (nb_ord, tid))
        c.execute("UPDATE tags SET ord=? WHERE id=?;", (cur_ord, nb_id))
        conn.commit(); conn.close()
        self.refresh_tags()
        for i in range(self.tag_list.count()):
            if self.tag_list.item(i).data(Qt.UserRole) == tid:
                self.tag_list.setCurrentRow(i); break

    def rename_selected_tag(self):
        cur = self.tag_list.currentItem()
        if not cur or cur.data(Qt.UserRole) is None:
            QMessageBox.information(self, "안내", "이름을 바꿀 태그를 선택하세요. (\"전체 파일\" 제외)")
            return
        old_tid = cur.data(Qt.UserRole)
        old_name = cur.text().rsplit(" (", 1)[0]
        new_name, ok = QInputDialog.getText(self, "태그 이름 변경", "새 태그 이름:", text=old_name)
        if not ok: return
        self._rename_or_merge_tag(old_tid, new_name.strip())

    def rename_tag_inline(self, item: QListWidgetItem):
        tid = item.data(Qt.UserRole)
        if tid is None:
            return
        old_name = item.text().rsplit(" (", 1)[0]
        new_name, ok = QInputDialog.getText(self, "태그 이름 변경", "새 태그 이름:", text=old_name)
        if not ok: return
        self._rename_or_merge_tag(tid, new_name.strip())

    def _rename_or_merge_tag(self, old_tid: int, new_name: str):
        if not new_name:
            return
        conn = get_conn(); c = conn.cursor()
        c.execute("SELECT id FROM tags WHERE name=?;", (new_name,))
        row = c.fetchone()
        if row:
            new_tid = row[0]
            if new_tid == old_tid:
                conn.close(); return
            c.execute("INSERT OR IGNORE INTO file_tags(file_id, tag_id) "
                      "SELECT file_id, ? FROM file_tags WHERE tag_id=?;", (new_tid, old_tid))
            c.execute("DELETE FROM file_tags WHERE tag_id=?;", (old_tid,))
            c.execute("DELETE FROM tags WHERE id=?;", (old_tid,))
            conn.commit(); conn.close()
            QMessageBox.information(self, "안내", f"동일 이름이 있어 태그를 병합했습니다: {new_name}")
        else:
            try:
                c.execute("UPDATE tags SET name=? WHERE id=?;", (new_name, old_tid))
                conn.commit()
            except sqlite3.IntegrityError:
                conn.close()
                QMessageBox.warning(self, "오류", "태그 이름이 중복되어 변경할 수 없습니다.")
                return
            conn.close()
        self.refresh_all_counts()

    # --------------------- 파일 목록/선택 ---------------------
    def refresh_files_and_save(self):
        self.save_state(); self.refresh_files()

    def refresh_files(self):
        self.table.setSortingEnabled(False)
        self.table.setRowCount(0)
        rows = list_files(
            self.search.text().strip(),
            list(self.selected_tag_ids),
            self.chk_only_tagged.isChecked(),
            self.root_filter
        )
        self.count_lbl.setText(f"결과: {len(rows):,}건")

        for fid, path, size, mtime, tag_text in rows:
            r = self.table.rowCount(); self.table.insertRow(r)
            fname = os.path.basename(path); fdir = os.path.dirname(path)

            it_file = QTableWidgetItem(fname); it_file.setData(Qt.UserRole, path)
            it_file.setToolTip(path)

            # ★ 파일 크기: 탐색기 스타일
            it_size = QTableWidgetItem(format_size_explorer(size))
            it_size.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)

            it_mtim = QTableWidgetItem(datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S"))
            it_tags = QTableWidgetItem(tag_text or "")
            it_dir  = QTableWidgetItem(fdir); it_dir.setToolTip(path)
            it_id   = QTableWidgetItem(str(fid))

            if tag_text:
                first_tag = (tag_text.split(",")[0] or "").strip()
                if first_tag:
                    col = self.tag_color_by_name.get(first_tag, color_for_tag(None, first_tag))
                    it_file.setBackground(QBrush(col))

            self.table.setItem(r, 0, it_file)
            self.table.setItem(r, 1, it_size)
            self.table.setItem(r, 2, it_mtim)
            self.table.setItem(r, 3, it_tags)
            self.table.setItem(r, 4, it_dir)
            self.table.setItem(r, 5, it_id)

        self.table.setSortingEnabled(True)
        self.table.sortItems(2, Qt.DescendingOrder)

    def selected_file_ids(self):
        ids = []
        if self.table.selectionModel():
            for idx in self.table.selectionModel().selectedRows():
                ids.append(int(self.table.item(idx.row(), 5).text()))
        return ids

    def get_selected_paths(self):
        paths = []
        if self.table.selectionModel():
            for idx in self.table.selectionModel().selectedRows():
                p = self.table.item(idx.row(), 0).data(Qt.UserRole)
                if p: paths.append(p)
        return paths

    # --------------------- 테이블 컨텍스트 메뉴/액션 ---------------------
    def on_table_context_menu(self, pos):
        if not self.table.itemAt(pos):
            return
        menu = QMenu(self)
        act_open = menu.addAction("열기")
        act_show = menu.addAction("폴더에서 보기")
        act_rename = menu.addAction("이름 바꾸기")
        act_delete = menu.addAction("삭제(휴지통)")
        act = menu.exec_(self.table.viewport().mapToGlobal(pos))
        if not act: return
        if act == act_open:
            ps = self.get_selected_paths()
            if ps:
                self._open_path(ps[0])
        elif act == act_show:
            for p in self.get_selected_paths():
                show_in_explorer(p)
        elif act == act_rename:
            self.rename_selected_file()
        elif act == act_delete:
            self.delete_selected_files()

    def _open_path(self, full_path: str):
        try:
            if sys.platform.startswith("win"):
                os.startfile(full_path)
            elif sys.platform == "darwin":
                os.system(f'open "{full_path}"')
            else:
                os.system(f'xdg-open "{full_path}"')
        except Exception as e:
            QMessageBox.warning(self, "오류", f"파일 열기 실패:\n{e}")

    def refresh_selected_file_tags(self):
        self.sel_tags.clear()
        ids = self.selected_file_ids()
        if len(ids) != 1: return
        for tid, name in list_file_tags(ids[0]):
            it = QListWidgetItem(name)
            it.setData(Qt.UserRole, tid)
            it.setBackground(QBrush(color_for_tag(tid, name)))
            self.sel_tags.addItem(it)

    def assign_tag_to_selected(self):
        name = self.combo_tag.currentText().strip()
        if not name: return
        tid = ensure_tag(name)
        ids = self.selected_file_ids()
        if not ids:
            QMessageBox.information(self, "안내", "파일을 먼저 선택하세요."); return
        conn = get_conn(); cur = conn.cursor()
        for fid in ids:
            cur.execute("INSERT OR IGNORE INTO file_tags(file_id, tag_id) VALUES(?, ?);", (fid, tid))
        conn.commit(); conn.close()
        self.refresh_all_counts()

    def untag_selected_from_selected_files(self):
        ids = self.selected_file_ids()
        if len(ids) != 1:
            QMessageBox.information(self, "안내", "오른쪽 목록은 단일 파일 선택 시만 조작됩니다."); return
        fid = ids[0]; selected = self.sel_tags.selectedItems()
        if not selected: return
        conn = get_conn(); cur = conn.cursor()
        for it in selected:
            cur.execute("DELETE FROM file_tags WHERE file_id=? AND tag_id=?;", (fid, it.data(Qt.UserRole)))
        conn.commit(); conn.close()
        self.refresh_all_counts()

    def rename_selected_file(self):
        ps = self.get_selected_paths()
        if len(ps) != 1:
            QMessageBox.information(self, "안내", "이름 변경은 한 개의 파일만 선택하세요.")
            return
        old = ps[0]
        new_name, ok = QInputDialog.getText(self, "이름 바꾸기", "새 파일명:",
                                            text=os.path.basename(old))
        if not ok or not new_name.strip():
            return
        new_path = os.path.join(os.path.dirname(old), new_name.strip())
        try:
            os.rename(old, new_path)
        except Exception as e:
            QMessageBox.warning(self, "오류", f"이름 바꾸기 실패:\n{e}")
            return
        db_rename_file_path(old, new_path)
        self.refresh_all_counts()

    def delete_selected_files(self):
        ps = self.get_selected_paths()
        if not ps:
            return
        if QMessageBox.question(self, "삭제 확인",
                                f"{len(ps)}개 파일을 삭제(휴지통)할까요?") != QMessageBox.StandardButton.Yes:
            return
        for p in ps:
            if recycle_delete(p):
                db_delete_file_by_path(p)
        self.refresh_all_counts()

    # --------------------- 루트/스캔 ---------------------
    def pick_root(self):
        if self._busy_forbid_if_running("스캔"):
            return
        d = QFileDialog.getExistingDirectory(self, "루트 폴더 선택")
        if d:
            self.root_dir = normalize_path(d)
            self.root_path_edit.setText(self.root_dir)
            set_setting("last_root", self.root_dir)
            add_root(self.root_dir)
            self.refresh_roots_panel()
            self.root_filter = None
            self.root_list.setCurrentRow(0)
            self.refresh_tags()
            self.refresh_files()

    def index_selected_root(self):
        root = self.root_dir
        if not root:
            it = self.root_list.currentItem()
            if it and it.data(Qt.UserRole):
                root = it.data(Qt.UserRole)
        if not root:
            QMessageBox.information(self, "안내", "먼저 [루트 선택]으로 색인할 폴더를 지정하세요.")
            return
        if self.scan_running:
            QMessageBox.information(self, "안내", "현재 다른 작업이 진행 중입니다.")
            return

        self.scan_running = True
        root = normalize_path(root)

        fs_total, fs_max_m = walk_count_and_max_mtime(root)
        db_total, db_max_m = db_count_and_max_mtime_under(root)
        if fs_total == db_total and (db_max_m >= fs_max_m):
            self.scan_finished.emit()
            return

        self.scan_started.emit(fs_total)
        step = max(1, fs_total // 100)

        def worker():
            processed = 0
            try:
                add_root(root)
                for dp, _, files in os.walk(root):
                    for fn in files:
                        full = os.path.join(dp, fn)
                        try:
                            upsert_file(full)
                        except Exception:
                            pass
                        processed += 1
                        if (processed % step == 0) or (processed == fs_total):
                            self.progress_tick.emit(processed, fs_total)
                remove_missing_under(root)
            finally:
                self.scan_finished.emit()

        threading.Thread(target=worker, daemon=True).start()

    def rescan_selected_root(self):
        if self._busy_forbid_if_running("재스캔"):
            return
        item = self.root_list.currentItem()
        if not item or item.data(Qt.UserRole) is None:
            QMessageBox.information(self, "안내", "재스캔할 경로를 선택하세요."); return
        root = item.data(Qt.UserRole)
        self.root_dir = root
        self.root_path_edit.setText(root)
        self.index_selected_root()

    def rescan_all_roots(self):
        if self._busy_forbid_if_running("전체 재스캔"):
            return
        roots = list_roots()
        if not roots:
            QMessageBox.information(self, "안내", "재스캔할 경로가 없습니다.")
            return

        uniq = list({normalize_path(r) for r in roots})
        total = 0
        for r in uniq:
            try:
                total += count_files_on_disk(r)
            except Exception:
                pass

        self.scan_running = True
        self.scan_started.emit(total if total > 0 else 1)

        def worker():
            processed = 0
            try:
                for root in uniq:
                    add_root(root)
                    for dp, _, files in os.walk(root):
                        for fn in files:
                            full = os.path.join(dp, fn)
                            try:
                                upsert_file(full)
                            except Exception:
                                pass
                            processed += 1
                            if processed % 500 == 0 or processed == total:
                                self.progress_tick.emit(processed, total if total > 0 else 1)
                    remove_missing_under(root)
            finally:
                self.scan_finished.emit()

        threading.Thread(target=worker, daemon=True).start()

    def remove_selected_root(self):
        if self._busy_forbid_if_running("제거"):
            return
        item = self.root_list.currentItem()
        if not item or item.data(Qt.UserRole) is None:
            QMessageBox.information(self, "안내", "제거할 경로를 선택하세요."); return
        root = item.data(Qt.UserRole)
        if QMessageBox.question(self, "경로 제거",
                                f"'{root}' 경로를 목록에서 제거하고 관련 파일 기록을 삭제할까요?") \
                == QMessageBox.StandardButton.Yes:
            remove_root(root)
            if self.root_filter == root:
                self.root_filter = None
            if self.root_path_edit.text() == root:
                self.root_path_edit.clear()
            if self.root_dir == root:
                self.root_dir = None
            self.refresh_roots_panel()
            self.root_list.setCurrentRow(0)
            self.refresh_files()

    # ★ 루트 목록 클릭 슬롯
    def on_root_clicked(self, item: QListWidgetItem):
        path = item.data(Qt.UserRole)
        self.root_filter = None if path is None else path
        self.refresh_files()

    # --------------------- 기타/헬퍼 ---------------------
    def on_only_tagged_toggled(self, _state):
        self.update_checkbox_style()
        self.refresh_files_and_save()

    def update_checkbox_style(self):
        if self.chk_only_tagged.isChecked():
            self.chk_only_tagged.setStyleSheet("color: #b00020; font-weight: 900;")
        else:
            self.chk_only_tagged.setStyleSheet("color: #1e1f23; font-weight: 800;")

    def _open_url(self, url: str):
        """기본 브라우저로 링크 열기."""
        try:
            webbrowser.open(url)
        except Exception as e:
            QMessageBox.warning(self, "오류", f"링크 열기 실패:\n{e}")

    def _busy_forbid_if_running(self, purpose: str) -> bool:
        if self.scan_running:
            QMessageBox.information(self, "안내", f"현재 다른 작업이 진행 중입니다.\n({purpose})가 끝난 후 다시 시도하세요.")
            return True
        return False

    def open_file(self, item):
        if item.column() != 0:
            return
        full_path = item.data(Qt.UserRole)
        if not full_path:
            return
        self._open_path(full_path)

# =========================[ 엔트리포인트 ]===================================
def main():
    init_db()
    app = QApplication(sys.argv)
    ui = MainUI(); ui.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
