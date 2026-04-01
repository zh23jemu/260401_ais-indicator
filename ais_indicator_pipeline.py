#!/usr/bin/env python3
"""
AIS indicator pipeline for three raw indicators:
1) ship traffic density   船舶通航密度
2) traffic flow complexity 交通流复杂度
3) regional conflict frequency 区域冲突频率
"""

from __future__ import annotations

import argparse
import glob
import math
import os
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

import geopandas as gpd
import numpy as np
import openpyxl
import pandas as pd
from pyproj import Transformer
from scipy.spatial import cKDTree
from shapely import wkt as shapely_wkt
from shapely.geometry import Point, Polygon

COLUMN_ALIASES = {
    "time": "timestamp",
    "ts": "timestamp",
    "datetime": "timestamp",
    "longitude": "lon",
    "lng": "lon",
    "latitude": "lat",
    "sog": "speed",
    "speed_over_ground": "speed",
    "cog": "course",
    "course_over_ground": "course",
}
REQUIRED_COLUMNS = ["timestamp", "mmsi", "lat", "lon", "speed", "course"]


# ---------------------------
# 直接内置你的5个区域坐标
# ---------------------------
def dms_to_decimal(dms_str):
    dms_str = dms_str.replace("′", "'").replace("″", "\"").strip()
    direction = dms_str[-1]
    num = dms_str[:-1]
    parts = num.replace("°", " ").replace("'", " ").replace('"', ' ').split()
    d = float(parts[0])
    m = float(parts[1]) if len(parts) >= 2 else 0
    s = float(parts[2]) if len(parts) >= 3 else 0
    dec = d + m / 60 + s / 3600
    return -dec if direction in ["S", "W"] else dec


def get_5_regions():
    r1 = [
        (dms_to_decimal("122°00'00\"E"), dms_to_decimal("36°16'00\"N")),
        (dms_to_decimal("122°13'40\"E"), dms_to_decimal("36°16'00\"N")),
        (dms_to_decimal("122°13'40\"E"), dms_to_decimal("36°23'00\"N")),
        (dms_to_decimal("122°07'00\"E"), dms_to_decimal("36°23'00\"N")),
    ]
    r2 = [
        (dms_to_decimal("121°13'38.5401\"E"), dms_to_decimal("36°29'13.3908\"N")),
        (dms_to_decimal("121°14'50.4489\"E"), dms_to_decimal("36°29'12.4510\"N")),
        (dms_to_decimal("121°16'31.3431\"E"), dms_to_decimal("36°27'45.8570\"N")),
        (dms_to_decimal("121°16'35.2853\"E"), dms_to_decimal("36°25'17.8750\"N")),
        (dms_to_decimal("121°15'00\"E"), dms_to_decimal("36°25'00\"N")),
        (dms_to_decimal("121°14'59.9980\"E"), dms_to_decimal("36°21'23.8750\"N")),
        (dms_to_decimal("121°14'22.2922\"E"), dms_to_decimal("36°21'22.0140\"N")),
        (dms_to_decimal("121°11'54.9987\"E"), dms_to_decimal("36°23'37.3358\"N")),
        (dms_to_decimal("121°11'55.0001\"E"), dms_to_decimal("36°26'20.6342\"N")),
    ]
    r3 = [
        (dms_to_decimal("121°23'27.33414\"E"), dms_to_decimal("36°19'45.78523\"N")),
        (dms_to_decimal("121°22'27.16339\"E"), dms_to_decimal("36°22'55.41008\"N")),
        (dms_to_decimal("121°32'37.04550\"E"), dms_to_decimal("36°26'06.13982\"N")),
        (dms_to_decimal("121°33'14.69578\"E"), dms_to_decimal("36°23'57.59279\"N")),
    ]
    r4 = [
        (dms_to_decimal("121°11′47″E"), dms_to_decimal("35°17′47″N")),
        (dms_to_decimal("121°33′40″E"), dms_to_decimal("35°00′30″N")),
        (dms_to_decimal("121°19′18″E"), dms_to_decimal("35°00′34″N")),
        (dms_to_decimal("121°10′36″E"), dms_to_decimal("35°17′48″N")),
    ]
    r5 = [
        (dms_to_decimal("120°45′42″E"), dms_to_decimal("35°08′46″N")),
        (dms_to_decimal("120°05′04″E"), dms_to_decimal("35°08′38″N")),
        (dms_to_decimal("120°06'35\"E"), dms_to_decimal("35°06'06\"N")),
        (dms_to_decimal("120°43'07\"E"), dms_to_decimal("35°05'52\"N")),
    ]
    feats = []
    for i, coords in enumerate([r1, r2, r3, r4, r5], 1):
        feats.append({"site_id": f"R{i}", "geometry": Polygon(coords)})
    gdf = gpd.GeoDataFrame(feats, crs="EPSG:4326")
    return gdf


# ====================== 自动填充参数，彻底解决报错 ======================
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ais", nargs="+", default=["4-24.xlsx"])
    parser.add_argument("--outdir", default="./ais_output")
    parser.add_argument("--sheet-name", default=None)
    parser.add_argument("--chunk-size", type=int, default=200000)
    parser.add_argument("--time-bin", default="10min")
    parser.add_argument("--max-speed-kn", type=float, default=40.0)
    parser.add_argument("--max-jump-kn", type=float, default=60.0)
    parser.add_argument("--heading-bins", type=int, default=8)
    parser.add_argument("--search-radius-nm", type=float, default=6.0)
    parser.add_argument("--dcpa-threshold-nm", type=float, default=0.5)
    parser.add_argument("--tcpa-max-min", type=float, default=30.0)
    parser.add_argument("--merge-window-min", type=float, default=20.0)
    parser.add_argument("--max-rows", type=int, default=None)
    return parser.parse_args()


# ====================== 以下全部保持原版逻辑不变 ======================
def expand_files(patterns):
    files = []
    for p in patterns:
        matches = sorted(glob.glob(p))
        if matches:
            files.extend(matches)
        elif os.path.exists(p):
            files.append(p)
    if not files:
        raise FileNotFoundError("无匹配文件")
    return sorted(dict.fromkeys(files))


def normalize_columns(df):
    rename = {}
    for c in df.columns:
        k = str(c).strip().lower()
        k = COLUMN_ALIASES.get(k, k)
        rename[c] = k
    df = df.rename(columns=rename)
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"缺失列: {missing}")
    return df.copy()


def iter_xlsx_chunks(path, sheet_name, chunk_size, max_rows):
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb[sheet_name] if sheet_name else wb.active
    rows = ws.iter_rows(values_only=True)
    header = [str(x).strip() if x else f"c{i}" for i, x in enumerate(next(rows))]
    batch = []
    n = 0
    for r in rows:
        batch.append(r)
        n += 1
        if max_rows and n >= max_rows:
            yield pd.DataFrame(batch, columns=header)
            batch = []
            break
        if len(batch) >= chunk_size:
            yield pd.DataFrame(batch, columns=header)
            batch = []
    if batch:
        yield pd.DataFrame(batch, columns=header)


def iter_ais_chunks(path, sheet_name, chunk_size, max_rows):
    ext = Path(path).suffix.lower()
    if ext == ".xlsx":
        yield from iter_xlsx_chunks(path, sheet_name, chunk_size, max_rows)
    else:
        raise ValueError("仅支持xlsx")


def infer_local_epsg(lon, lat):
    zone = int((lon + 180) // 6) + 1
    return 32600 + zone if lat >= 0 else 32700 + zone


def load_sites():
    sites_wgs = get_5_regions()
    merged_geometry = (
        sites_wgs.geometry.union_all()
        if hasattr(sites_wgs.geometry, "union_all")
        else sites_wgs.geometry.unary_union
    )
    centroid = merged_geometry.centroid
    epsg = infer_local_epsg(centroid.x, centroid.y)
    sites_proj = sites_wgs.to_crs(epsg)
    sites_proj["area_km2"] = sites_proj.geometry.area / 1e6
    cent_wgs = sites_proj.geometry.centroid.to_crs(4326)
    sites_wgs["area_km2"] = sites_proj["area_km2"].values
    sites_wgs["centroid_lon"] = cent_wgs.x.values
    sites_wgs["centroid_lat"] = cent_wgs.y.values
    return sites_wgs, epsg


def unix_to_datetime(series):
    series = pd.to_numeric(series, errors="coerce")
    med = series.median()
    unit = "ms" if med > 1e12 else "s"
    return pd.to_datetime(series, unit=unit, utc=True, errors="coerce")


def haversine_nm(lat1, lon1, lat2, lon2):
    R = 6371.0088
    lat1, lat2 = np.radians(lat1), np.radians(lat2)
    dlat = lat2 - lat1
    dlon = np.radians(lon2 - lon1)
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    c = 2 * np.arcsin(np.sqrt(a))
    return R * c / 1.852


def clean_ais_chunk(df, bbox, time_bin, max_speed_kn, max_jump_kn):
    df = normalize_columns(df)
    for c in ["mmsi", "lat", "lon", "speed", "course"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["ts"] = unix_to_datetime(df["timestamp"])
    df = df.dropna(subset=["ts", "mmsi", "lat", "lon", "speed", "course"])
    df = df[(df.mmsi.between(1e8, 1e9 - 1)) & df.lat.between(-90, 90) & df.lon.between(-180, 180)]
    df = df[df.speed.between(0, max_speed_kn) & df.course.between(0, 360)]
    if bbox:
        x1, y1, x2, y2 = bbox
        df = df[(df.lon >= x1) & (df.lon <= x2) & (df.lat >= y1) & (df.lat <= y2)]
    if df.empty:
        return df
    df = df.sort_values(["mmsi", "ts"]).drop_duplicates(["mmsi", "ts"], keep="first")
    prev_lat = df.groupby("mmsi").lat.shift()
    prev_lon = df.groupby("mmsi").lon.shift()
    prev_ts = df.groupby("mmsi").ts.shift()
    dt = (df.ts - prev_ts).dt.total_seconds() / 3600
    dist = haversine_nm(prev_lat, prev_lon, df.lat, df.lon)
    jump = (dt > 0) & (dt < 1) & (dist / dt > max_jump_kn)
    df = df[~jump.fillna(False)]
    df["time_bin"] = df.ts.dt.floor(time_bin)
    df = df.sort_values(["mmsi", "time_bin", "ts"]).drop_duplicates(["mmsi", "time_bin"], keep="first")
    return df[["mmsi", "ts", "time_bin", "lat", "lon", "speed", "course"]].copy()


# ---------------------------
# 修复关键报错：空间匹配索引对齐
# ---------------------------
def attach_sites(df, sites_wgs):
    if df.empty:
        return df
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.lon, df.lat), crs="EPSG:4326")
    out = []
    for _, site in sites_wgs.iterrows():
        pip = gdf.within(site.geometry)
        if pip.any():
            sub = gdf[pip].copy()
            sub["site_id"] = site.site_id
            sub["site_area_km2"] = site.area_km2
            out.append(sub)
    if not out:
        return pd.DataFrame()
    final = pd.concat(out, ignore_index=True)
    final = final.drop(columns=["geometry"])
    return final


def heading_entropy(courses, n_bins):
    v = courses.dropna()
    if len(v) < 2:
        return 0.0
    counts, _ = np.histogram(v, bins=np.linspace(0, 360, n_bins + 1))
    counts = counts[counts > 0]
    p = counts / counts.sum()
    return -(p * np.log(p)).sum() / np.log(n_bins)


def timeslice_metrics(joined, heading_bins):
    if joined.empty:
        return pd.DataFrame(
            columns=["site_id", "time_bin", "site_area_km2", "vessel_count", "density_raw", "heading_entropy",
                     "speed_mean_kn", "speed_std_kn", "speed_cv", "complexity_raw"])

    def agg(g):
        n = g.mmsi.nunique()
        mu = g.speed.mean() if n else 0
        std = g.speed.std(ddof=0) if n > 1 else 0
        cv = std / mu if mu > 0.5 else 0
        ent = heading_entropy(g.course, heading_bins)
        comp = 0.5 * ent + 0.5 * min(cv, 2) / 2
        return pd.Series(
            {"vessel_count": n, "heading_entropy": ent, "speed_mean_kn": mu, "speed_std_kn": std, "speed_cv": cv,
             "complexity_raw": comp})

    g = joined.groupby(["site_id", "time_bin", "site_area_km2"], as_index=False).apply(agg, include_groups=False)
    g["density_raw"] = g.vessel_count / g.site_area_km2
    return g


def project_xy(df, epsg):
    if df.empty:
        return df
    t = Transformer.from_crs(4326, epsg, always_xy=True)
    x, y = t.transform(df.lon, df.lat)
    df = df.copy()
    df["x_m"], df["y_m"] = x, y
    return df


def detect_conflicts(proj, search_nm, dcpa_nm, tcpa_min):
    if proj.empty or len(proj) < 2:
        return pd.DataFrame(columns=["site_id", "time_bin", "mmsi_1", "mmsi_2", "dcpa_nm", "tcpa_min"])
    rep = proj.sort_values(["site_id", "time_bin", "mmsi"]).drop_duplicates(["site_id", "time_bin", "mmsi"],
                                                                            keep="first")
    events = []
    s_m = search_nm * 1852
    d_m = dcpa_nm * 1852
    t_s = tcpa_min * 60
    for (sid, tb), g in rep.groupby(["site_id", "time_bin"]):
        if len(g) < 2: continue
        xy = g[["x_m", "y_m"]].values
        tree = cKDTree(xy)
        pairs = tree.query_pairs(s_m)
        if not pairs: continue
        crs = np.radians(g.course.to_numpy())
        spd = g.speed.to_numpy() * 0.514444
        vx, vy = spd * np.sin(crs), spd * np.cos(crs)
        mms = g.mmsi.to_numpy()
        for i, j in pairs:
            dr = xy[j] - xy[i]
            dv = np.array([vx[j] - vx[i], vy[j] - vy[i]])
            vv = dv @ dv
            if vv < 1e-8: continue
            t = -dr @ dv / vv
            if t < 0 or t > t_s: continue
            dc = np.linalg.norm(dr + dv * t)
            if dc <= d_m:
                m1, m2 = sorted([int(mms[i]), int(mms[j])])
                events.append({"site_id": sid, "time_bin": tb, "mmsi_1": m1, "mmsi_2": m2, "dcpa_nm": dc / 1852,
                               "tcpa_min": t / 60})
    return pd.DataFrame(events)


def dedup_conflicts(ev, win_min):
    if ev.empty: return ev
    ev = ev.sort_values(["site_id", "mmsi_1", "mmsi_2", "time_bin"])
    ev["key"] = ev.site_id.astype(str) + "_" + ev.mmsi_1.astype(str) + "_" + ev.mmsi_2.astype(str)
    keep = []
    last = {}
    for r in ev.itertuples():
        k = r.key
        now = pd.Timestamp(r.time_bin)
        if k not in last or (now - last[k]).total_seconds() / 60 > win_min:
            keep.append(True)
            last[k] = now
        else:
            keep.append(False)
    return ev[keep].drop(columns=["key"]).reset_index(drop=True)


# ====================== MAIN ======================
def main():
    args = parse_args()
    out = Path(args.outdir)
    out.mkdir(parents=True, exist_ok=True)
    sites, epsg = load_sites()
    x1, y1, x2, y2 = sites.total_bounds
    bbox = (x1 - 0.5, y1 - 0.5, x2 + 0.5, y2 + 0.5)
    ts_list = []
    cf_list = []
    wrote_sample = False
    tmin = tmax = None
    for f in expand_files(args.ais):
        for ck in iter_ais_chunks(f, args.sheet_name, args.chunk_size, args.max_rows):
            cl = clean_ais_chunk(ck, bbox, args.time_bin, args.max_speed_kn, args.max_jump_kn)
            if cl.empty: continue
            if not wrote_sample:
                cl.head(2000).to_csv(out / "cleaned_sample.csv", index=False)
                wrote_sample = True
            curr_min, curr_max = cl.ts.min(), cl.ts.max()
            tmin = curr_min if tmin is None else min(tmin, curr_min)
            tmax = curr_max if tmax is None else max(tmax, curr_max)
            j = attach_sites(cl, sites)
            if j.empty: continue
            ts = timeslice_metrics(j, args.heading_bins)
            if not ts.empty:
                ts_list.append(ts)
            pj = project_xy(j, epsg)
            cf = detect_conflicts(pj, args.search_radius_nm, args.dcpa_threshold_nm, args.tcpa_max_min)
            if not cf.empty:
                cf_list.append(cf)
    if not ts_list:
        raise RuntimeError("无有效数据落入区域")
    ts_all = pd.concat(ts_list, ignore_index=True)
    ts_all = ts_all.groupby(["site_id", "time_bin", "site_area_km2"], as_index=False).agg({
        "vessel_count": "sum", "heading_entropy": "mean", "speed_mean_kn": "mean", "speed_std_kn": "mean",
        "speed_cv": "mean", "complexity_raw": "mean", "density_raw": "mean"
    })
    ts_all.to_csv(out / "site_timeslice_metrics.csv", index=False)
    cf_all = dedup_conflicts(pd.concat(cf_list, ignore_index=True),
                             args.merge_window_min) if cf_list else pd.DataFrame()
    cf_all.to_csv(out / "conflict_events.csv", index=False)
    days = (tmax - tmin).total_seconds() / 86400 if tmin and tmax else 1
    summary = ts_all.groupby(["site_id", "site_area_km2"], as_index=False).agg({
        "density_raw": "mean", "complexity_raw": "mean"
    })
    cf_cnt = cf_all.groupby("site_id").size().reset_index(name="conflicts_total") if not cf_all.empty else pd.DataFrame(
        columns=["site_id", "conflicts_total"])
    final = summary.merge(cf_cnt, on="site_id", how="left").fillna({"conflicts_total": 0})
    final["conflict_freq_per_day"] = final.conflicts_total / days
    final = final[["site_id", "density_raw", "complexity_raw", "conflict_freq_per_day"]].sort_values("site_id")
    final.to_csv(out / "site_indicator_raw.csv", index=False)

    print("=" * 50)
    print("运行成功。")
    print("结果已保存到: ais_output 文件夹")
    print("三个核心指标:")
    print(" 1. density_raw = 船舶通航密度")
    print(" 2. complexity_raw = 交通流复杂度")
    print(" 3. conflict_freq_per_day = 区域冲突频率")
    print("=" * 50)


if __name__ == "__main__":
    main()
