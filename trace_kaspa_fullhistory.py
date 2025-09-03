import requests
import time
import os
import json
import pandas as pd
from datetime import datetime, timezone

API_BASE = "https://api.kaspa.org"
DATA_DIR = "flow_data_fullhistory"
os.makedirs(DATA_DIR, exist_ok=True)

# These are the addresses to crawl at the L1 layer: Top‑20 SLOW holders
# (as reconstructed from Kasplex KRC20 ops, accepted only, 8 decimals) plus the
# two known brt2412 wallets (deployer + qranu...). Order preserved; duplicates removed.

"""
ROOTS = [
    "kaspa:qp0eszr5v7mnkdmrr98lvyu3276vsv6hhc0r9s82j0rc8thgzzn6g7tra2jem",
    "kaspa:qrjfrcnhvwulqtt7kg9vwh6hgzd9c64tmdrpeql7m64zkqcj2cf6wnc3v7mf2",
    "kaspa:qrxu0k37978rkgg2q2aawpxely8gnc04pqmqhqyrtrls2rt2ym2hchdu5qv0m",
    "kaspa:qzt7p2a6yfcnc6hurwtaauq2vcdzzjjdnd0chq06aw4z987tka327p94qcsjs",
    "kaspa:qzhev2xnjhtz8yqhnsglp6w5eyuv4hm3xxznuqenmkcwuh6hu5qly83yskddp",
    "kaspa:qppf6h8p7k2dw9ly9c0hmj8cgguya73jae2zcmk00qnc3eqg8hw5ut3nj5xuv",
    "kaspa:qzsf049846rau9zydgf6qgerf6p0jyqtzdmvpsum2s622lsyq54ws2eta9clj",
    "kaspa:qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqkx9awp4e",  # burn address
    "kaspa:qqgnhx3us5tkqhe7szvqdlpf5j7jqkr8z4u07ywr0du6kj7zw4nd7xa6ll2zc",
    "kaspa:qranuazvj2tz7cf750hrj0msfp29addevej7gr98qkhn6vjrkmngjrh3alqf6",  # brt2412 (wallet)
    "kaspa:qypt0cuckk7qrpvnnvgc6f69nl9qas0axw5vecjcf2s46gann5zqhuqptlr7nhc",
    "kaspa:qpequela06ngp3l82x07naeq0w76ml5yzaz9hz5vn2whjfex24qfy2ye45jax",
    "kaspa:qrcgrvtt2ugt3fv4kxfu4jwrgne8lw0ke6tt5chfwkvywdwekefwwh0ew7zhw",
    "kaspa:qqldh8umxwmamaht380awgsx7skvep3gumm5fe9hywjl44mp7rpmjm84xkhe5",
    "kaspa:qr5xy7w4esspt25tza5zxqflthgrlww7063l8cxl2vng6lz3kkaqc87mz6s4g",
    "kaspa:qrfl9lp90590m3qnuy7r77zqmh4wn7t5evh8ml55f3x0j7q822st59j7xsrrg",
    "kaspa:qypja2cc0ks62l8udqvdzmjr5gvytc8xxgwtuulvg7754cma4wdm3mswtavlwrq",
    "kaspa:qrdgcz9wf6crz4lumalmtwazxndaw0r3kujmv0qjar9exahzjahyke9njgn3u",
    "kaspa:qqx8n487cefv50gy0kpx9qqsamfcev0ctuudfc54svmkvfx72zgj7tpymc677",
    "kaspa:qrthr4upnk3aucrsq7cg37mkhu82smjzw0l3mwgrslcqtn265nd57mrhn60s5",
    "kaspa:qq5x0cka3ku08sp3tfcffeznvhyfxzrwkpj8pqhs2sllwgzva7ej2j8tpa4ty",   # brt2412 (deployer)
]
"""

# 80 SLOW minter wallets
ROOTS = [
    "kaspa:qp0eszr5v7mnkdmrr98lvyu3276vsv6hhc0r9s82j0rc8thgzzn6g7tra2jem",
    "kaspa:qp0mwshtjy0d5llclyxfakjh5sus28shuzhajpp3ljlvh5ndzpr2q73wn5fv8",
    "kaspa:qp0vzuzlw4mrqwu5v86n5kt62pgf8hmtwks52wlwwqps2z94v5hskfm0vev6m",
    "kaspa:qp3fmzsueusx4rvtzdx4dqyhhxrfqcmv9l09ryl7hwm6ggdm8m8kvmj4lanhs",
    "kaspa:qp44k58nmtwkufg6y8usyjsw2nzf4cfkeqv6hqr3nzy7zs7fl0y9z2k8g9slm",
    "kaspa:qp4vr8xyuqmh8yjdtzgzt9s88appyfakduhn7efuef9tw7v9f3qhsmhavd2tj",
    "kaspa:qp773260zac504zaa82kd34r9f0cqfcvvn78z6qcwzm2pkz9gtv2cvj3u92vy",
    "kaspa:qp86wp4plg94qefe968y9tnzxwc5ays2c8vuskrsycya5flq8ezju7dg6pc2r",
    "kaspa:qp9pkt53d9ece5h6gddkhxjhj83w7ylh5tltsv209gat6a9egy3zvsu3pwrnp",
    "kaspa:qpa94qn7jrqxjcagp7l9ttzndtnplhcvl2js4772a27t86ypwmr566ud9yn9e",
    "kaspa:qpcgde8dyfk4e4446qywha72nxvmjaq0u7segkqzqcgdwg6wkmwukqvy540jx",
    "kaspa:qpde09lxfhk5nsywelrvtq4au2c98t72lj02d3q32g298dcfp2tpjv8w8h95a",
    "kaspa:qpequela06ngp3l82x07naeq0w76ml5yzaz9hz5vn2whjfex24qfy2ye45jax",
    "kaspa:qpfjlqc044ccyfmnd9lsp9e3fyalqg46q8e4axuq6na9hs4qgdk3vnr06gch9",
    "kaspa:qpl7peyjh2k38g6e7ujmy5fpa952nfjn3q2u4cznqxtmtyssfwnesxd0dangh",
    "kaspa:qpmtlt9f7yp5tgmtlhm7w2rsqylc3npr9wfmmk7hlwkxnzx72rvk7vk7wvz29",
    "kaspa:qpqjquwuky7qkd82s9fl533g0880ghddtvmw0jw0rlr2uvaquggh7ahsr8zvd",
    "kaspa:qprx39wwj8h9a5ww78rdgn6dqaedeyuvnw6zqs470qsqf60tqy657ufdrzj6l",
    "kaspa:qpxku6vxyp2t5yacjhv3crd8unl4yd7r559azrrugn3e42ytsqsrw7ffr468c",
    "kaspa:qq20cleamwdznqwt20dlqsst8qyhm5acxlpyhuwxvthwpmel5j7wktm4c84fd",
    "kaspa:qq227pzm0shdlnx05ngavhpu7vypk7ftknu072zq0y5dgvcd633kj49lx5taz",
    "kaspa:qq5x0cka3ku08sp3tfcffeznvhyfxzrwkpj8pqhs2sllwgzva7ej2j8tpa4ty",
    "kaspa:qq7a89sy4xku99e8yed8zm8644rv3hgfxl8s4lgam4acetptdtun29f0mz2c4",
    "kaspa:qq83ww5263cns3g6ezvakwukaq7t0gj9785ymg0n946fn6ea9pmvs3hdqqkg9",
    "kaspa:qqce3azurzjzuekkmds3wf3la887pu835d5ftxvs93vld8u0vj40ch2m88wf2",
    "kaspa:qqmhxwmj7rgt8gtuzpxjf4zhncg6vu3zwpxfhfvqcsx5wkndky74wy36v3ynm",
    "kaspa:qqpguw5wnscq4z9weqhxkkrs95w6v909l6djxfd96mjk44vfwqv9w0f4ddnmc",
    "kaspa:qqq26h33t7m8z9zu64j8m8f5g5rp3dztkvhptm68630p8ykq3mux2ed38s2uf",
    "kaspa:qqqv9n6h04q85ggl4tzgk96c2h6734yrw40wgfcgktyzrnlx36ypyjmargcyl",
    "kaspa:qqrm2u8wum2q3u4mlvx65gzmzc4xw6pmzukxhfvegxp33z2kpcd5sft74v4f7",
    "kaspa:qqt3xlmrppy34rcmeew6azfr90kw3x5mtxnl2x7ceyd68w3kp9afjgd3f4758",
    "kaspa:qqv6zsxk0k9c2580zualugf2xm85rstffts3u4wd3madcexul9d9gw6ea92pw",
    "kaspa:qqvh5lf6pm28q2auv8ljjh75ts04glwfvs8xu0h03s5tuas0y29nj3909af4m",
    "kaspa:qqw7rvydnqrnq6skw7gag9dv40qgf2qmvr9s5x3zcx3z8xewst94zx5at9ks6",
    "kaspa:qqwqeu76ansr802yv4fj9xlp7y94uhfhn642mqkx6s75vv43yd042r3hd8ex3",
    "kaspa:qr3uun38alarzalz0n7dzmzqeusc9zg7lttsyf9pz3qgdq4ns8pwv4u7ql8ga",
    "kaspa:qr5m73n4qnmre86rf220z080fqjly7jdxzfth8gzkcmpjkxyxc39kj6526wd5",
    "kaspa:qr5pfrs3ja2t4m8e5yq7tl08r6c5d9l69z42a0dyar86qxa7apfec7l68hpah",
    "kaspa:qr5xy7w4esspt25tza5zxqflthgrlww7063l8cxl2vng6lz3kkaqc87mz6s4g",
    "kaspa:qr663j9at2uepn07m72gyw8nvfpmx87xnxgjeljw47f7pumydq8xjd7py9mld",
    "kaspa:qr78mh8l75kdmpxcac6hyw82kl453l2mpnmsspjkqhnzhxxme3ktwrdstgz52",
    "kaspa:qr7gv6ecdglnjmtqvc3krtxnshgp93fyahhkeg9xrddn2xna0ycfq2lgkzylt",
    "kaspa:qr80yl9a5xlhg9rz2nfc6x689s6hva6kft7s8y3pl0pzca3etsc2jt6s4rcx3",
    "kaspa:qrdthz88ghd2fasasqcynhttunnhzhqxey3t2cjal5uj48966l3nucmt5mzyq",
    "kaspa:qrf3s57pnaqcm8xzeq36znhprzv3fc0n9yfn59v8e9dhm6ne20mjxe7rde009",
    "kaspa:qrfl2xarll42e0ml8pd38ckc388pjk26tm0avdrpzqkup62vqm9yc4rvmd5nf",
    "kaspa:qrfzj84vel5utykhs234rpgny0c42rmml7ddlprtyt2h0qf6ldx8g50awa9c6",
    "kaspa:qrj2qu6gglfkhev3f3vld3kqc49yrd5jweuumd8yrgf6a9menrzmj94r56lks",
    "kaspa:qrkqwj6tzag3kldenlnmr3d3t7kmr6sxxe3anvszect2qeq5ykgpyxwata7yu",
    "kaspa:qrmsh6facj8s4l4jw7xf7klnpjvf5m4v5ryrptyv80rgfccnrtmfg5cuwe67l",
    "kaspa:qrqgzsnxlaeu3alrwz9ksysyahlghqm7zxcax7thr0er5srxupfqydjucqwwq",
    "kaspa:qrvk0rp3e4k9lw3sz7aapes2jzss8r5n6mthratcqjtxcvkqckjw288wkyf0g",
    "kaspa:qrze3glsylzsmqmepu7yy25yv70e77gdadfspcrt3g42fqar8lk6xd26gs90s",
    "kaspa:qz0n82x8g6xxx8zgeuww0mjvpuk8f5pat37jtv6l530ngfnatdht22xta48lw",
    "kaspa:qz8v4zne0j0lpmcja5qt6vk0vl57e8mgn3c7e5f2dwpaa6na25n9v30f35qpy",
    "kaspa:qz8ynf277z3d89twvhsxpymsvawtu334hjl4qs978uqhmq7y286vjx2w6kda9",
    "kaspa:qz9kjqn2r60y9dqg7d66yrcf5vmnfyzd3tlxn9nlvzr40wy935fkum4cgrcac",
    "kaspa:qza7ldvy2efdcygtpxmvk9p4ejxrnlr0e944kwy3fmyfzncwnrdk2x7xkvdv6",
    "kaspa:qzall06x422sk8gw4myhxc802vgnj4p2kvnj24t7d4ar985xuurdzhqm8ghzv",
    "kaspa:qzdf30stccllvgp4rgl3dwxrdh383ghqed438mwcf7t8qancplmh2ffnmjdcu",
    "kaspa:qzdznegxyf6dz7ftw9nrp0tuj9h4httvncfpu6wneusjaytzsxaesx2m90w2f",
    "kaspa:qzfgkp0d4t2v942nnp589u72zk7jlzaf4h3aye77y60fcl9sgppa73ftv07ld",
    "kaspa:qzglg3rl9k6fuc7capfp5zw3sjl3tj2q6jjdlhs8j70p2u98cvqhym20x9lmz",
    "kaspa:qzjjxtknensmpj72yzwnx0tw8n2hdlnvp8dsf39qg8ev0e4unk0dctls39f0a",
    "kaspa:qzk82r0t6c7zckc6qmw53r35te87xl2eftvqkxkqm936s4h5vmsaxp67ppu5e",
    "kaspa:qzkaphk6hvz860gvr4jdlfjn3q60umfw9vlq5mnhfqypjjw24qnm20s4wutc6",
    "kaspa:qzlwd3alwn78dcfpvgruwm86p0zpvf8t26fs3e7udts4vhagxefrzm5jdc72d",
    "kaspa:qzmmmcdkr6882gnygs04jrszj95e8fzyc0lj3f54j0j878g5lkcsga9an3ede",
    "kaspa:qzn29emflgvea47f63axr780qurrllye2jas7kt2ww9js4uzy23zxy3knw05a",
    "kaspa:qznrcn0k6swlp4nl8mjqcqgje95c43pp0r79vzdtcglh6x4agccygtlqxvyce",
    "kaspa:qzpkjy3y4tkhwuwchl7j4myyv7kx8t7vxaj4vw4z46tuzwptw97xu8h9sa57d",
    "kaspa:qzpzt5epww0hz0hzh2hc7flld5kadmx4j6yaa5u74yleelxpcz3uuk4c24wdu",
    "kaspa:qzqthcpz3gscg8plnr9xay7qn6msq0cllarlyhyprru5wax2e0tpw0t76vjuy",
    "kaspa:qzs88e42zj670wtru556ug4vvuedskh5c5sy2ujjc59wc4q5f2rf224kw8kwp",
    "kaspa:qztevr7u8xu8hkyej4snfsqj47f5r6h2wdzwg95a3cvmperu4qr4q3kgkk3yx",
    "kaspa:qzv7v52024jng947m9jgzua6z94shqswtfaqtafaf96ga7mrxcdzspm0zrqvm",
    "kaspa:qzvr5djct4j3ehat4qvr3yc2w6era9g4llylql2dr3pgekg5h3gexjfrd20cc",
    "kaspa:qzw3mtq8f2l4r0fxrukzuv7cgwcgpanc83h90g3zq3f8px7axfndz4kdjd0gz",
    "kaspa:qzwkwp0gaqkee00fq4cz3gnvgnz46lfue9lukfz8n7t3cvf0v83k7sl5jzv4q",
    "kaspa:qzxth5fn3uenclnr724mhw37y6lwdkufhjxe9nzecxyz0pvltkd3vczzf2t0d",
]


def format_timestamp(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()

CUTOFF_DATE = datetime(2022, 1, 1)

def is_before_cutoff(timestamp):
    try:
        dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        return dt < CUTOFF_DATE
    except Exception as e:
        print("Timestamp parsing failed:", timestamp)
        return False

def fetch_transactions(address, max_pages=100000):
    records = []
    before = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    foundcutoff = False
    
    for _ in range(max_pages):
        url = (
            f"{API_BASE}/addresses/{address}/full-transactions-page"
            f"?limit=500&before={before}&resolve_previous_outpoints=full&acceptance=accepted"
        )
        print(f"📦 Fetching before={before} for {address}")
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"❌ Error fetching transactions: {e}")
            break

        if not isinstance(data, list) or not data:
            print("✅ No more transactions.")
            break

        for tx in data:
            if not isinstance(tx, dict):
                print(f"⚠️ Skipping non-dict entry: {tx}")
                continue

            tx_id = tx.get("transaction_id", tx.get("txId", "UNKNOWN"))
            timestamp = format_timestamp(tx.get("block_time", 0))
            inputs = tx.get("inputs") or []
            # inputs = tx.get("inputs", [])
            # outputs = tx.get("outputs", [])
            outputs = tx.get("outputs") or []

            if is_before_cutoff(timestamp):
                print("Reached cutoff date at:", timestamp)
                foundcutoff = True
                break
            
            for inp in inputs:
                sender = inp.get("previous_outpoint_address", "UNKNOWN")
                for out in outputs:
                    recipient = out.get("script_public_key_address", "UNKNOWN")
                    amount_kas = int(out.get("amount", 0)) / 1e8
                    records.append({
                        "tx_id": tx_id,
                        "timestamp": timestamp,
                        "sender": sender,
                        "recipient": recipient,
                        "amount_kas": amount_kas
                    })

        if foundcutoff: break
        
        before = data[-1].get("block_time", before)

    return pd.DataFrame(records)

def fetch_transactions_all_participants(address, max_pages=100000):
    """
    Fetch transactions involving the specified address, but return all inputs and outputs
    from those transactions — regardless of whether each individual input/output is related to the address.
    """
    records = []
    before = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

    for _ in range(max_pages):
        url = (
            f"{API_BASE}/addresses/{address}/full-transactions-page"
            f"?limit=500&before={before}&resolve_previous_outpoints=full&acceptance=accepted"
        )
        print(f"📦 Fetching before={before} for {address}")
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"❌ Error fetching transactions: {e}")
            break

        if not isinstance(data, list) or not data:
            print("✅ No more transactions.")
            break

        for tx in data:
            if not isinstance(tx, dict):
                print(f"⚠️ Skipping non-dict entry: {tx}")
                continue

            tx_id = tx.get("transaction_id", tx.get("txId", "UNKNOWN"))
            timestamp = format_timestamp(tx.get("block_time", 0))
            inputs = tx.get("inputs") or [] # inputs = tx.get("inputs", [])
            outputs = tx.get("outputs") or [] # outputs = tx.get("outputs", [])

            # Build full input set (transaction-level context)
            input_summary = {}
            for inp in inputs:
                sender = inp.get("previous_outpoint_address", "UNKNOWN")
                input_summary[sender] = input_summary.get(sender, 0) + int(inp.get("previous_outpoint_amount", 0))

            total_input_sompi = sum(input_summary.values())
            if total_input_sompi == 0:
                continue  # avoid divide-by-zero

            # For each output, record proportional attribution from each sender
            for out in outputs:
                recipient = out.get("script_public_key_address", "UNKNOWN")
                amount_sompi = int(out.get("amount", 0))
                for sender, contribution in input_summary.items():
                    weight = contribution / total_input_sompi
                    records.append({
                        "tx_id": tx_id,
                        "timestamp": timestamp,
                        "sender": sender,
                        "recipient": recipient,
                        "amount_kas": amount_sompi * weight / 1e8
                    })

        before = data[-1].get("block_time", before)

    return pd.DataFrame(records)

def trace_wallet(address):
    print(f"🔍 Fetching full transaction set for {address} (all participants mode)")
    txs = fetch_transactions_all_participants(address)
    print(f"📥 {len(txs)} total sender→recipient records collected")

    txs_filtered = txs
    
    try:
        # Only keep transactions involving the address directly
        txs_filtered = txs[(txs["sender"] == address) | (txs["recipient"] == address)]
        print(f"🔎 {len(txs_filtered)} filtered records where {address} was sender or recipient")
    except:
        print('no transactions')
        
    # Save the full and filtered dataset
    full_outpath = os.path.join(DATA_DIR, f"{address.replace(':', '_')}_all_participants.csv")
    filtered_outpath = os.path.join(DATA_DIR, f"{address.replace(':', '_')}_involving.csv")

    txs.to_csv(full_outpath, index=False)
    txs_filtered.to_csv(filtered_outpath, index=False)

    print(f"✅ Saved full transaction data to {full_outpath}")
    print(f"✅ Saved filtered personal transaction data to {filtered_outpath}")
    
if __name__ == "__main__":
    for addr in ROOTS:
        trace_wallet(addr)
    print("✅ Completed full non-recursive transaction history export.")
