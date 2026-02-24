import asyncio
from playwright.async_api import async_playwright
import sqlite3
import json
from datetime import datetime
import os

async def update_database():
    """تحديث القاعدة - كل مباريات الموسم"""
    
    db_path = 'data/saudi_league_complete.db'
    os.makedirs('data', exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("🔄 بدء التحديث...")
    print("="*60)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        page = await context.new_page()
        
        headers = {
            'Accept': 'application/json',
            'Referer': 'https://www.sofascore.com/',
        }
        
        TID = 955
        SID = 80443
        
        # ================================
        # 1. جلب كل المباريات
        # ================================
        print("\n🏟️  جلب كل مباريات الموسم...")
        
        all_events = []
        page_num = 0
        
        while page_num < 10:
            r = await page.request.get(
                f"https://www.sofascore.com/api/v1/unique-tournament/{TID}/season/{SID}/events/last/{page_num}",
                headers=headers
            )
            
            if r.status != 200:
                break
            
            data = await r.json()
            events = data.get('events', [])
            
            if not events:
                break
            
            all_events.extend(events)
            page_num += 1
            await asyncio.sleep(1)
        
        print(f"  📊 وجدت {len(all_events)} مباراة")
        
        # ================================
        # 2. حفظ المباريات المنتهية
        # ================================
        print("\n💾 حفظ المباريات...")
        
        new_matches = 0
        updated_matches = 0
        
        for event in all_events:
            if event['status']['type'] != 'finished':
                continue
            
            match_id = event['id']
            match_date = datetime.fromtimestamp(event['startTimestamp']).strftime('%Y-%m-%d %H:%M:%S')
            
            cursor.execute("SELECT match_id FROM matches WHERE match_id = ?", (match_id,))
            exists = cursor.fetchone()
            
            cursor.execute("""
            INSERT OR REPLACE INTO matches 
            (match_id, season_id, round, date_time, home_team_id, away_team_id,
             home_score, away_score, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                match_id, SID,
                event.get('roundInfo', {}).get('round'),
                match_date,
                event['homeTeam']['id'],
                event['awayTeam']['id'],
                event['homeScore'].get('current'),
                event['awayScore'].get('current'),
                event['status']['type']
            ))
            
            if exists:
                updated_matches += 1
            else:
                new_matches += 1
                
                await asyncio.sleep(1.5)
                
                r2 = await page.request.get(
                    f"https://www.sofascore.com/api/v1/event/{match_id}/lineups",
                    headers=headers
                )
                
                if r2.status == 200:
                    lineups = await r2.json()
                    
                    for side in ['home', 'away']:
                        team_id = event['homeTeam']['id'] if side == 'home' else event['awayTeam']['id']
                        
                        for player_data in lineups.get(side, {}).get('players', []):
                            player = player_data.get('player', {})
                            stats = player_data.get('statistics', {})
                            
                            if player and stats:
                                cursor.execute("""
                                INSERT OR REPLACE INTO player_match_stats
                                (match_id, player_id, team_id, minutes_played, rating,
                                 goals, assists, stats_json)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                """, (
                                    match_id,
                                    player['id'],
                                    team_id,
                                    stats.get('minutesPlayed', 0),
                                    stats.get('rating', 0),
                                    stats.get('goals', 0),
                                    stats.get('goalAssist', 0),
                                    json.dumps(stats)
                                ))
                
                print(f"  ✅ جديدة: {event['homeTeam']['name']} vs {event['awayTeam']['name']}")
        
        conn.commit()
        print(f"\n  📊 {new_matches} مباراة جديدة")
        print(f"  🔄 {updated_matches} مباراة محدثة")
        
        # ================================
        # 3. تحديث إحصائيات الموسم
        # ================================
        print("\n📊 تحديث إحصائيات الموسم...")
        
        groups = ['summary', 'attack', 'defence', 'passing']
        all_player_stats = {}
        
        for group in groups:
            print(f"  → {group}...")
            
            offset = 0
            while offset < 200:
                r = await page.request.get(
                    f"https://www.sofascore.com/api/v1/unique-tournament/{TID}/season/{SID}/statistics",
                    headers=headers,
                    params={
                        'limit': 100,
                        'offset': offset,
                        'order': '-rating',
                        'accumulation': 'total',
                        'group': group,
                    }
                )
                
                if r.status != 200:
                    break
                
                data = await r.json()
                results = data.get('results', [])
                
                if not results:
                    break
                
                for item in results:
                    player = item.pop('player', {})
                    team = item.pop('team', {})
                    pid = player['id']
                    
                    if pid not in all_player_stats:
                        all_player_stats[pid] = {
                            'player_id': pid,
                            'team_id': team.get('id'),
                            **item
                        }
                    else:
                        all_player_stats[pid].update(item)
                
                if len(results) < 100:
                    break
                
                offset += 100
                await asyncio.sleep(1)
            
            await asyncio.sleep(1.5)
        
        for pid, stats in all_player_stats.items():
            cursor.execute("""
            INSERT OR REPLACE INTO player_season_stats 
            (player_id, season_id, team_id, appearances, minutes_played, rating, 
             goals, assists, stats_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                pid, SID,
                stats.get('team_id'),
                stats.get('appearances', 0),
                stats.get('minutesPlayed', 0),
                stats.get('rating', 0),
                stats.get('goals', 0),
                stats.get('assists', 0),
                json.dumps(stats)
            ))
        
        conn.commit()
        print(f"  ✅ {len(all_player_stats)} لاعب")
        
        await browser.close()
    
    conn.close()
    
    print("\n🎉 اكتمل التحديث!")
    print("="*60)

if __name__ == "__main__":
    asyncio.run(update_database())