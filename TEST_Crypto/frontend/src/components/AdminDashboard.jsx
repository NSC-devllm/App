import React, { useEffect, useState } from 'react';
import api from '../api';

const AdminDashboard = () => {
    const [ledgers, setLedgers] = useState([]);
    const [orders, setOrders] = useState([]);

    const fetchData = async () => {
        try {
            const [ledgerRes, orderRes] = await Promise.all([
                api.get('/admin/ledger'),
                api.get('/admin/orders')
            ]);
            setLedgers(ledgerRes.data);
            setOrders(orderRes.data);
        } catch (err) {
            console.error(err);
        }
    };

    useEffect(() => {
        fetchData();
        const interval = setInterval(fetchData, 2000);
        return () => clearInterval(interval);
    }, []);

    return (
        <div>
            <h2>Admin Dashboard</h2>
            <p style={{ color: '#666', fontSize: '0.8rem' }}>Real-time database view</p>

            <div className="card">
                <h3>Transaction Ledger (Immutable Log)</h3>
                <div style={{ maxHeight: '300px', overflowY: 'auto' }}>
                    <table className="ledger-table">
                        <thead>
                            <tr>
                                <th>Type</th>
                                <th>Amount</th>
                                <th>Wallet</th>
                                <th>Time</th>
                            </tr>
                        </thead>
                        <tbody>
                            {ledgers.map((l) => (
                                <tr key={l.tx_id}>
                                    <td>{l.type}</td>
                                    <td>{l.amount}</td>
                                    <td style={{ fontSize: '0.7rem' }}>{l.wallet_id}</td>
                                    <td style={{ fontSize: '0.7rem' }}>{new Date(l.created_at).toLocaleTimeString()}</td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            </div>

            <div className="card">
                <h3>Payment Orders State</h3>
                <table className="ledger-table">
                    <thead>
                        <tr>
                            <th>Order ID</th>
                            <th>Status</th>
                            <th>Amount</th>
                        </tr>
                    </thead>
                    <tbody>
                        {orders.map((o) => (
                            <tr key={o.order_id}>
                                <td style={{ fontSize: '0.7rem', fontFamily: 'monospace' }}>{o.order_id.substring(0, 8)}...</td>
                                <td>
                                    <span className={`status-badge status-${o.status}`}>
                                        {o.status}
                                    </span>
                                </td>
                                <td>{o.amount}</td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>
        </div>
    );
};

export default AdminDashboard;
