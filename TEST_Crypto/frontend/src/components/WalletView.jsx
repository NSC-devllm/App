import React, { useEffect, useState } from 'react';
import api from '../api';

const WalletView = ({ userId = 'test_user_01' }) => {
    const [wallet, setWallet] = useState(null);
    const [loading, setLoading] = useState(false);

    const fetchWallet = async () => {
        setLoading(true);
        try {
            const res = await api.get(`/wallet/${userId}`);
            setWallet(res.data);
        } catch (err) {
            console.error(err);
            alert('Failed to load wallet');
        } finally {
            setLoading(false);
        }
    };

    const handleBuy = async () => {
        const amount = prompt("Enter amount of NSC to buy (KRW):", "10000");
        if (!amount) return;
        try {
            await api.post('/buy', { user_id: userId, amount: parseFloat(amount) });
            fetchWallet();
        } catch (err) {
            alert('Buy failed');
        }
    };

    useEffect(() => {
        fetchWallet();
        const interval = setInterval(fetchWallet, 3000); // Auto refresh
        return () => clearInterval(interval);
    }, [userId]);

    if (!wallet) return <div>Loading...</div>;

    return (
        <div>
            <div className="card" style={{ background: 'linear-gradient(135deg, #03C75A 0%, #093687 100%)', color: 'white' }}>
                <div style={{ fontSize: '0.9rem', opacity: 0.8 }}>Total Balance</div>
                <div style={{ fontSize: '2rem', fontWeight: 800, margin: '10px 0' }}>
                    {wallet.balance.toLocaleString()} NSC
                </div>
                <div style={{ fontSize: '0.9rem', opacity: 0.9 }}>
                    Frozen: {wallet.frozen_amount.toLocaleString()} NSC
                </div>
            </div>

            <button onClick={handleBuy} className="btn btn-primary" style={{ marginBottom: '20px' }}>
                + Charge NSC
            </button>

            <h3>Recent Transactions</h3>
            <div className="card">
                {wallet.transactions.length === 0 ? (
                    <p style={{ color: '#888', textAlign: 'center' }}>No transactions yet</p>
                ) : (
                    <table className="ledger-table">
                        <thead>
                            <tr>
                                <th>Type</th>
                                <th>Amount</th>
                                <th>Time</th>
                            </tr>
                        </thead>
                        <tbody>
                            {wallet.transactions.map((tx) => (
                                <tr key={tx.tx_id}>
                                    <td>
                                        <span style={{
                                            fontWeight: 600,
                                            color: tx.type === 'BUY' ? '#d60000' : tx.type === 'REFUND' ? '#007bff' : '#333'
                                        }}>
                                            {tx.type}
                                        </span>
                                    </td>
                                    <td className={tx.type === 'BUY' || tx.type === 'REFUND' ? 'amount plus' : 'amount minus'}>
                                        {tx.type === 'BUY' || tx.type === 'REFUND' ? '+' : '-'}{Number(tx.amount).toLocaleString()}
                                    </td>
                                    <td style={{ fontSize: '0.7rem', color: '#aaa' }}>
                                        {new Date(tx.created_at).toLocaleTimeString()}
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                )}
            </div>
        </div>
    );
};

export default WalletView;
