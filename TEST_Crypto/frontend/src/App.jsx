import React from 'react';
import { BrowserRouter as Router, Routes, Route, Link, useLocation } from 'react-router-dom';
import WalletView from './components/WalletView';
import PaymentDemo from './components/PaymentDemo';
import AdminDashboard from './components/AdminDashboard';
import './index.css';

const Navbar = () => {
  const location = useLocation();

  return (
    <nav className="navbar">
      <Link to="/" className="brand">
        <span className="n">N</span>aver <span className="f">F</span>inance
      </Link>
      <div className="nav-links">
        <Link to="/" className={`nav-item ${location.pathname === '/' ? 'active' : ''}`}>Wallet</Link>
        <Link to="/pay" className={`nav-item ${location.pathname === '/pay' ? 'active' : ''}`}>Pay</Link>
        <Link to="/admin" className={`nav-item ${location.pathname === '/admin' ? 'active' : ''}`}>Admin</Link>
      </div>
    </nav>
  );
};

function App() {
  return (
    <Router>
      <div className="app-container">
        <Navbar />
        <div className="content">
          <Routes>
            <Route path="/" element={<WalletView />} />
            <Route path="/pay" element={<PaymentDemo />} />
            <Route path="/admin" element={<AdminDashboard />} />
          </Routes>
        </div>
      </div>
    </Router>
  );
}

export default App;
