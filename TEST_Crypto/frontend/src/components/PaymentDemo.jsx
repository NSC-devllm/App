import React, { useState } from 'react';
import api from '../api';
import { useNavigate } from 'react-router-dom';

const PaymentDemo = ({ userId = 'test_user_01' }) => {
    const [loading, setLoading] = useState(false);
    const navigate = useNavigate();

    const handlePayment = async () => {
        setLoading(true);
        try {
            // 1. Prepare
            const amount = 5000; // Coffee price
            const prepareRes = await api.post('/pay/prepare', {
                user_id: userId,
                merchant_name: 'Starbucks Gangnam',
                amount: amount
            });

            const { order_id } = prepareRes.data;

            // Simulate network delay or user confirmation
            const confirm = window.confirm(`Payment Prepared.\nFrozen Amount: ${amount}\nConfirm payment?`);

            if (confirm) {
                // 2. Confirm
                await api.post('/pay/confirm', { order_id });
                alert('Payment Success! Frozen amount settled.');
                navigate('/');
            } else {
                // 3. Cancel
                await api.post('/pay/cancel', { order_id });
                alert('Payment Canceled. Frozen amount refunded.');
            }

        } catch (err) {
            console.error(err);
            alert('Payment Failed: ' + (err.response?.data?.detail || err.message));
        } finally {
            setLoading(false);
        }
    };

    return (
        <div style={{ textAlign: 'center', padding: '20px' }}>
            <h2>Merchant Payment Demo</h2>
            <div className="card">
                <div style={{ fontSize: '3rem', marginBottom: '10px' }}>☕</div>
                <h3>Iced Americano</h3>
                <p style={{ fontSize: '1.2rem', fontWeight: 'bold' }}>5,000 NSC</p>
                <p style={{ color: '#888' }}>Starbucks Gangnam</p>
            </div>

            <button
                className="btn btn-secondary"
                onClick={handlePayment}
                disabled={loading}
            >
                {loading ? 'Processing...' : 'Pay with Naver Pay'}
            </button>

            <p style={{ marginTop: '20px', fontSize: '0.8rem', color: '#666' }}>
                * Clicking Pay will first "Freeze" your assets, then "Settle" them upon confirmation.
            </p>
        </div>
    );
};

export default PaymentDemo;
